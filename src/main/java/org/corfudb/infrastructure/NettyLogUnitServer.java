package org.corfudb.infrastructure;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import lombok.Data;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.corfudb.infrastructure.wireprotocol.*;
import org.corfudb.runtime.smr.Pair;
import org.corfudb.util.retry.IRetry;
import org.corfudb.util.retry.IntervalAndSentinelRetry;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
public class NettyLogUnitServer extends AbstractNettyServer {

    ConcurrentHashMap<java.util.UUID, Long> trimMap;
    Thread gcThread;
    IntervalAndSentinelRetry gcRetry;
    boolean streamAware;

    // Optimization for maintaining the "sublog ordering invariant"
    private HashMap<UUID, RangeSet<Long>> streamRangeMap = new HashMap();
    private HashMap<UUID, HashMap<Range<Long>, Pair<Long, Long>>> rangeToGlobalMap = new HashMap();

    public NettyLogUnitServer(boolean streamAware) {
        this.streamAware = streamAware;
    }

    @Data
    @RequiredArgsConstructor
    public class LogUnitEntry implements IMetadata {
        final ByteBuf buffer;
        final EnumMap<LogUnitMetadataType, Object> metadataMap;
        final boolean isHole;

        /** Generate a new log unit entry which is a hole */
        public LogUnitEntry()
        {
            buffer = null;
            metadataMap = new EnumMap<>(LogUnitMetadataType.class);
            isHole = true;
        }
        
	/** Get the streams that belong to this entry.
         *
         * @return A set of streams that belong to this entry.
         */
        @SuppressWarnings("unchecked")
        public Set<UUID> getStreams()
        {
            return (Set<UUID>) metadataMap.getOrDefault(NettyLogUnitServer.LogUnitMetadataType.STREAM,
                    Collections.EMPTY_SET);
        }

        /** Set the streams that belong to this entry.
         *
         * @param streams The set of belong to this entry.
         */
        public void setStreams(Set<UUID> streams)
        {
            metadataMap.put(NettyLogUnitServer.LogUnitMetadataType.STREAM, streams);
        }

        /** Get the rank of this entry.
         *
         * @return The rank of this entry.
         */
        @SuppressWarnings("unchecked")
        public Long getRank()
        {
            return (Long) metadataMap.getOrDefault(NettyLogUnitServer.LogUnitMetadataType.RANK,
                    0L);
        }

        /** Set the rank of this entry.
         *
         * @param rank The rank of this entry.
         */
        public void setRank(Long rank)
        {
            metadataMap.put(NettyLogUnitServer.LogUnitMetadataType.RANK, rank);
        }

        public void setCommit(boolean commit) {metadataMap.put(LogUnitMetadataType.COMMIT, commit); }

    }

    @RequiredArgsConstructor
    public enum LogUnitMetadataType {
        STREAM(0),
        RANK(1),
        STREAM_ADDRESS(2),
        COMMIT(3)
        ;

        final int type;

        public byte asByte() { return (byte)type; }
    }

    public static Map<Byte, LogUnitMetadataType> metadataTypeMap =
            Arrays.<LogUnitMetadataType>stream(LogUnitMetadataType.values())
                    .collect(Collectors.toMap(LogUnitMetadataType::asByte, Function.identity()));

    @RequiredArgsConstructor
    public enum ReadResultType {
        EMPTY(0),
        DATA(1),
        FILLED_HOLE(2),
        TRIMMED(3)
        ;

        final int type;

        public byte asByte() { return (byte)type; }
    }

    public static Map<Byte, ReadResultType> readResultTypeMap =
            Arrays.<ReadResultType>stream(ReadResultType.values())
                    .collect(Collectors.toMap(ReadResultType::asByte, Function.identity()));


    /**
     * This cache services requests for data at various addresses. In a memory implementation,
     * it is not backed by anything, but in a disk implementation it is backed by persistent storage.
     */
    LoadingCache<Long, LogUnitEntry> dataCache;
    LoadingCache<Pair, LogUnitEntry> streamDataCache;

    /**
     * This cache services requests for hints.
     */
    //Cache<Long, Set<NewLogUnitHints>> hintCache;

    /**
     * The contiguous head of the log (that is, the lowest address which has NOT been trimmed yet).
     */
    @Getter
    long contiguousHead;

    /**
     * A range set representing trimmed addresses on the log unit.
     */
    RangeSet<Long> trimRange;

    @Override
    public void close() {
        if (gcThread != null)
        {
            gcThread.interrupt();
        }
        /** Free all references */
        if (!streamAware) {
            dataCache.asMap().values().parallelStream()
                    .map(m -> m.buffer.release());
        } else {
            streamDataCache.asMap().values().parallelStream()
                    .map(m -> m.buffer.release());
        }
        super.close();
    }

    @Override
    void parseConfiguration(Map<String, Object> configuration)
    {
        serverName = "NettyLogUnitServer";
        reset();
        gcThread = new Thread(this::runGC);
        gcThread.start();
    }

    /**
     * Process an incoming message
     *
     * @param msg The message to process.
     * @param ctx The channel context from the handler adapter.
     */
    @Override
    @SuppressWarnings("unchecked")
    void processMessage(NettyCorfuMsg msg, ChannelHandlerContext ctx) {
        switch(msg.getMsgType())
        {
            case WRITE:
                write((NettyLogUnitWriteMsg) msg, ctx);
            break;
            case READ_REQUEST:
                read((NettyLogUnitReadRequestMsg) msg, ctx);
            break;
            case GC_INTERVAL:
            {
                NettyLogUnitGCIntervalMsg m = (NettyLogUnitGCIntervalMsg) msg;
                gcRetry.setRetryInterval(m.getInterval());
            }
            break;
            case FORCE_GC:
            {
                gcThread.interrupt();
            }
            break;
            case FILL_HOLE:
            {
                NettyLogUnitFillHoleMsg m = (NettyLogUnitFillHoleMsg) msg;
                dataCache.get(m.getAddress(), (address) -> new LogUnitEntry());
                //TODO: What about the streamDataCache?
            }
            break;
            case TRIM:
            {
                NettyLogUnitTrimMsg m = (NettyLogUnitTrimMsg) msg;
                trimMap.compute(m.getStreamID(), (key, prev) ->
                        prev == null ? m.getPrefix() : Math.max(prev, m.getPrefix()));
            }
            case SET_COMMIT:
            {
                setCommit((NettyLogUnitCommitMsg) msg, ctx);
            }
            break;
            case RESET:
            {
                reset(); // TODO: Look at the ResetMsg and get the latest epoch?
            }
        }
    }

    /**
     * Reset the state of the server.
     */
    @Override
    public void reset() {
        contiguousHead = 0L;
        trimRange = TreeRangeSet.create();

        if (!streamAware) {
            if (dataCache != null) {
                /** Free all references */
                dataCache.asMap().values().parallelStream()
                        .map(m -> m.buffer.release());
            }
            // Currently, only an in-memory configuration is supported.
            dataCache = Caffeine.newBuilder()
                    .build((a) -> {
                        return null;
                    });
        } else {
            if (streamDataCache != null) {
                /** Free all references */
                streamDataCache.asMap().values().parallelStream()
                        .map(m -> m.buffer.release());
            }
            // Currently, only an in-memory configuration is supported.
            streamDataCache = Caffeine.newBuilder()
                    .build((a) -> {
                        return null;
                    });
        }

        // Hints are always in memory and never persisted.
        /*
        hintCache = Caffeine.newBuilder()
                .weakKeys()
                .build();
*/
        // Trim map is set to empty on start
        // TODO: persist trim map - this is optional since trim is just a hint.
        trimMap = new ConcurrentHashMap<>();
    }

    /** Service an incoming read request. */
    public void read(NettyLogUnitReadRequestMsg msg, ChannelHandlerContext ctx)
    {
        if (trimRange.contains (msg.getAddress()))
        {
            sendResponse(new NettyLogUnitReadResponseMsg(ReadResultType.TRIMMED), msg, ctx);
        }
        else
        {
            if (!streamAware) {
                assert(msg.getAddress() != -1L);
                LogUnitEntry e = dataCache.get(msg.getAddress());
                if (e == null) {
                    sendResponse(new NettyLogUnitReadResponseMsg(ReadResultType.EMPTY), msg, ctx);
                } else if (e.isHole) {
                    sendResponse(new NettyLogUnitReadResponseMsg(ReadResultType.FILLED_HOLE), msg, ctx);
                } else {
                    sendResponse(new NettyLogUnitReadResponseMsg(e), msg, ctx);
                }
            } else {
                assert(msg.getAddress() == -1L);
                LogUnitEntry e = streamDataCache.get(new Pair(msg.getStream(), msg.getLocalAddress()));
                if (e == null) {
                    sendResponse(new NettyLogUnitReadResponseMsg(ReadResultType.EMPTY), msg, ctx);
                } else if (e.isHole) {
                    sendResponse(new NettyLogUnitReadResponseMsg(ReadResultType.FILLED_HOLE), msg, ctx);
                } else {
                    sendResponse(new NettyLogUnitReadResponseMsg(e), msg, ctx);
                }
            }
        }
    }

    // address is the global (physical) address and the streams Map contains local (logical) stream addresses
    synchronized private NettyCorfuMsg.NettyCorfuMsgType consensusDecision(long address, Map<UUID, Long> streams) {
        HashMap<UUID, Pair<Range<Long>, Pair<Long, Long>>> temp = new HashMap();
        for (UUID stream : streams.keySet()) {
            // All ranges in the RangeSet are assumed to be OPEN!!
            Range<Long> range;
            Pair<Long, Long> interval;
            if (streamRangeMap.get(stream) == null) {
                // This stream hasn't been written to end. Then its default range is (-infty, infty)
                range = Range.open(Long.MIN_VALUE, Long.MAX_VALUE);
                interval = new Pair<Long, Long>(Long.MIN_VALUE, Long.MAX_VALUE);
            } else {
                range = streamRangeMap.get(stream).rangeContaining(streams.get(stream));
                if (range == null)
                    return NettyCorfuMsg.NettyCorfuMsgType.ERROR_OVERWRITE;
                interval = rangeToGlobalMap.get(stream).get(range);
            }
            if (!(interval.first < address && interval.second > address))
                return NettyCorfuMsg.NettyCorfuMsgType.ERROR_SUBLOG;
            temp.put(stream, new Pair(range, interval));
        }
        // If this is a valid commit, we need to update the ranges in all the streams
        for (UUID stream : temp.keySet()) {
            Range<Long> oldRange = temp.get(stream).first;
            Pair<Long, Long> oldInterval = temp.get(stream).second;
            // Delete the old range
            if (streamRangeMap.get(stream) == null) {
                streamRangeMap.put(stream, TreeRangeSet.create());
                rangeToGlobalMap.put(stream, new HashMap());
            } else {
                streamRangeMap.get(stream).remove(oldRange);
                rangeToGlobalMap.get(stream).remove(oldRange);
            }

            // Insert two new ranges, if they aren't degenerate
            Long lower = oldRange.lowerEndpoint();
            Long upper = oldRange.upperEndpoint();
            Long newEndpoint = streams.get(stream);
            if (lower+1 != newEndpoint) {
                streamRangeMap.get(stream).add(Range.open(lower, newEndpoint));
                rangeToGlobalMap.get(stream).put(Range.open(lower, newEndpoint), new Pair<Long, Long>(oldInterval.first, address));
            }
            if (newEndpoint+1 != upper) {
                streamRangeMap.get(stream).add(Range.open(newEndpoint, upper));
                rangeToGlobalMap.get(stream).put(Range.open(newEndpoint, upper), new Pair<Long, Long>(address, oldInterval.second));
            }
        }
        return NettyCorfuMsg.NettyCorfuMsgType.ERROR_OK;
    }

    /** Service an incoming write request. */
    public void write(NettyLogUnitWriteMsg msg, ChannelHandlerContext ctx)
    {
        //TODO: locking of trimRange.
        if (trimRange.contains (msg.getAddress()))
        {
            sendResponse(new NettyCorfuMsg(NettyCorfuMsg.NettyCorfuMsgType.ERROR_TRIMMED), msg, ctx);
        }
        else {
            LogUnitEntry e = new LogUnitEntry(msg.getData(), msg.getMetadataMap(), false);
            e.getBuffer().retain();
            if (!streamAware) {
                if (e == dataCache.get(msg.getAddress(), (address) -> e)) {
                    sendResponse(new NettyCorfuMsg(NettyCorfuMsg.NettyCorfuMsgType.ERROR_OK), msg, ctx);
                } else {
                    e.getBuffer().release();
                    sendResponse(new NettyCorfuMsg(NettyCorfuMsg.NettyCorfuMsgType.ERROR_OVERWRITE), msg, ctx);
                }
            } else {
                // run the consensus decision first
                // Zip  together the streams/logids. TODO: These could be out of order..
                Map<UUID, Long> streams = new HashMap<UUID, Long>();
                int i = 0;
                for (UUID stream : (Set<UUID>) msg.getMetadataMap().get(LogUnitMetadataType.STREAM)) {
                    streams.put(stream, ((List<Long>) msg.getMetadataMap().get(LogUnitMetadataType.STREAM_ADDRESS)).get(i));
                    i++;
                }
                switch (consensusDecision(msg.getAddress(), streams)) {
                    case ERROR_OK:
                        break;
                    case ERROR_OVERWRITE:
                        e.getBuffer().release();
                        sendResponse(new NettyCorfuMsg(NettyCorfuMsg.NettyCorfuMsgType.ERROR_OVERWRITE), msg, ctx);
                        return;
                    case ERROR_SUBLOG:
                        e.getBuffer().release();
                        sendResponse(new NettyCorfuMsg(NettyCorfuMsg.NettyCorfuMsgType.ERROR_SUBLOG), msg, ctx);
                        return;
                    default:
                        break;
                }

                if (e == streamDataCache.get(new Pair(((Set<UUID>)(msg.getMetadataMap().get(LogUnitMetadataType.STREAM))).iterator().next(),
                        ((List<Long>)msg.getMetadataMap().get(LogUnitMetadataType.STREAM_ADDRESS)).get(0)), (address) -> e)) {
                    sendResponse(new NettyCorfuMsg(NettyCorfuMsg.NettyCorfuMsgType.ERROR_OK), msg, ctx);
                } else {
                    e.getBuffer().release();
                    sendResponse(new NettyCorfuMsg(NettyCorfuMsg.NettyCorfuMsgType.ERROR_OVERWRITE), msg, ctx);
                }
            }
        }
    }

    public void setCommit(NettyLogUnitCommitMsg msg, ChannelHandlerContext ctx) {
        if (trimRange.contains(msg.getAddress())) {
            sendResponse(new NettyCorfuMsg(NettyCorfuMsg.NettyCorfuMsgType.ERROR_TRIMMED), msg, ctx);
        } else {
            if (!streamAware) {
                LogUnitEntry currentEntry = dataCache.get(msg.getAddress());
                if (currentEntry == null)
                    log.info("Tried to set commit bit on null entry.. address: {}", msg.getAddress());
                currentEntry.setCommit(msg.isCommit());
                dataCache.put(msg.getAddress(), currentEntry);
            } else {
                for (UUID stream : msg.getStreams().keySet()) {
                    LogUnitEntry currentEntry = streamDataCache.get(new Pair(stream, msg.getStreams().get(stream)));
                    if (currentEntry == null)
                        log.info("Tried to set commit bit on null entry.. stream: {}, localAddr: {}, est. size: {}",
                                stream, msg.getStreams().get(stream), streamDataCache.estimatedSize());

                    currentEntry.setCommit(msg.isCommit());
                    streamDataCache.put(new Pair(stream, msg.getStreams().get(stream)), currentEntry);
                }
            }
            sendResponse(new NettyCorfuMsg(NettyCorfuMsg.NettyCorfuMsgType.ERROR_OK), msg, ctx);
        }
    }

    public void runGC()
    {
        Thread.currentThread().setName("LogUnit-GC");
        val retry = IRetry.build(IntervalAndSentinelRetry.class, this::handleGC)
                .setOptions(x -> x.setSentinelReference(running))
                .setOptions(x -> x.setRetryInterval(60_000));

        gcRetry = (IntervalAndSentinelRetry) retry;

        retry.runForever();
    }

    @SuppressWarnings("unchecked")
    public boolean handleGC()
    {
        log.info("Garbage collector starting...");
        long freedEntries = 0;

        /* Pick a non-compacted region or just scan the cache */
        Map<Long, LogUnitEntry> map = dataCache.asMap();
        SortedSet<Long> addresses = new TreeSet<>(map.keySet());
        for (long address : addresses)
        {
            LogUnitEntry buffer = dataCache.getIfPresent(address);
            if (buffer != null)
            {
                Set<UUID> streams = buffer.getStreams();
                // this is a normal entry
                if (streams.size() > 0) {
                    boolean trimmable = true;
                    for (java.util.UUID stream : streams)
                    {
                        Long trimMark = trimMap.getOrDefault(stream, null);
                        // if the stream has not been trimmed, or has not been trimmed to this point
                        if (trimMark == null || address > trimMark) {
                            trimmable = false;
                            break;
                        }
                        // it is not trimmable.
                    }
                    if (trimmable) {
                        trimEntry(address, streams, buffer);
                        freedEntries++;
                    }
                }
                else {
                    //this is an entry which belongs in all streams
                    //find the minimum contiguous range - and see if this entry is after it.
                    Range<Long> minRange = (Range<Long>) trimRange.complement().asRanges().toArray()[0];
                    if (minRange.contains(address))
                    {
                        trimEntry(address, streams, buffer);
                        freedEntries++;
                    }
                }
            }
        }

        log.info("Garbage collection pass complete. Freed {} entries", freedEntries);
        return true;
    }

    public void trimEntry(long address, Set<java.util.UUID> streams, LogUnitEntry entry)
    {
        // Add this entry to the trimmed range map.
        trimRange.add(Range.closed(address, address));
        // Invalidate this entry from the cache. This will cause the CacheLoader to free the entry from the disk
        // assuming the entry is back by disk
        dataCache.invalidate(address);
        //and free any references the buffer might have
        if (entry.getBuffer() != null)
        {
            entry.getBuffer().release();
        }
    }




}
