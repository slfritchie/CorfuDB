package org.corfudb.runtime.view;

import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.logprotocol.StreamCOWEntry;
import org.corfudb.protocols.wireprotocol.ILogUnitEntry;
import org.corfudb.protocols.wireprotocol.IMetadata;
import org.corfudb.protocols.wireprotocol.LogUnitReadResponseMsg;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.SequencerClient;
import org.corfudb.runtime.exceptions.OverwriteException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.NavigableSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * Created by mwei on 12/11/15.
 */
@Slf4j
public class StreamView implements AutoCloseable {

    /**
     * The ID of the stream.
     */
    @Getter
    final UUID streamID;
    final NavigableSet<StreamContext> streamContexts;
    CorfuRuntime runtime;

    public StreamView(CorfuRuntime runtime, UUID streamID) {
        this.runtime = runtime;
        this.streamID = streamID;
        this.streamContexts = new ConcurrentSkipListSet<>();
        this.streamContexts.add(new StreamContext(streamID, Long.MAX_VALUE));
    }

    public long getLogPointer() {
        return getCurrentContext().logPointer.get();
    }

    public void setLogPointer(long pos) {
        getCurrentContext().logPointer.set(pos);
    }

    public StreamContext getCurrentContext() {
        return streamContexts.first();
    }

    /**
     * Write an object to this stream, returning the physical address it
     * was written at.
     * <p>
     * Note: While the completion of this operation guarantees that the write
     * has been persisted, it DOES NOT guarantee that the object has been
     * written to the stream. For example, another client may have deleted
     * the stream.
     *
     * @param object The object to write to the stream.
     * @return The address this
     */
    public long write(Object object) {
        return acquireAndWrite(object, null, null);
    }

    /**
     * Write an object to this stream, returning the physical address it
     * was written at.
     * <p>
     * Note: While the completion of this operation guarantees that the write
     * has been persisted, it DOES NOT guarantee that the object has been
     * written to the stream. For example, another client may have deleted
     * the stream.
     *
     * @param object                The object to write to the stream.
     * @param acquisitionCallback   A function which will be called after the successful acquisition
     *                              of a token, but before the data is written.
     * @param deacquisitionCallback A function which will be called after an overwrite error is encountered
     *                              on a previously acquired token.
     * @return The address this object was written at.
     */
    public long acquireAndWrite(Object object, Function<SequencerClient.TokenResponse, Boolean> acquisitionCallback,
                                Function<SequencerClient.TokenResponse, Boolean> deacquisitionCallback) {
        while (true) {
            System.out.printf("%%%%%%%% !Stream!View use %s 1\n", runtime.toString());
            SequencerClient.TokenResponse tokenResponse =
                    runtime.getSequencerView().nextToken(Collections.singleton(streamID), 1);
            long token = tokenResponse.getToken();
            log.trace("Write[{}]: acquired token = {}", streamID, token);
            if (acquisitionCallback != null) {
                if (!acquisitionCallback.apply(tokenResponse)) {
                    log.trace("Acquisition rejected token, hole filling acquired address.");
                    try {
                        System.out.printf("%%%%%%%% !Stream!View use %s 2\n", runtime.toString());
                        runtime.getAddressSpaceView().fillHole(token);
                    } catch (OverwriteException oe) {
                        log.trace("Hole fill completed by remote client.");
                    }
                    return -1L;
                }
            }
            try {
                System.out.printf("%%%%%%%% !Stream!View use %s 3\n", runtime.toString());
                runtime.getAddressSpaceView().write(token, Collections.singleton(streamID),
                        object, tokenResponse.getBackpointerMap());
                return token;
            } catch (OverwriteException oe) {
                if (deacquisitionCallback != null && !deacquisitionCallback.apply(tokenResponse)) {
                    log.trace("Acquisition rejected overwrite at {}, not retrying.", token);
                    return -1L;
                }
                log.debug("Overwrite occurred at {}, retrying.", token);
            }
        }
    }

    /**
     * Returns the last issued token for this stream.
     *
     * @return The last issued token for this stream.
     */
    public long check() {
        System.out.printf("%%%%%%%% !Stream!View use %s 4\n", runtime.toString());return runtime.getSequencerView().nextToken(Collections.singleton(streamID), 0).getToken();
    }

    /**
     * Resolve a list of entries, using backpointers, to read.
     *
     * @param read The current address we are reading from.
     * @return A list of entries that we have resolved for reading.
     */
    public NavigableSet<Long> resolveBackpointersToRead(UUID streamID, long read) {
        System.out.printf("%%%%%%%% !Stream!View use %s 5\n", runtime.toString());
        long latestToken = runtime.getSequencerView().nextToken(Collections.singleton(streamID), 0).getToken();
        log.trace("Read[{}]: latest token at {}", streamID, latestToken);
        if (latestToken < read) {
            return new ConcurrentSkipListSet<>();
        }
        NavigableSet<Long> resolvedBackpointers = new ConcurrentSkipListSet<>();
        boolean hitStreamStart = false;
        boolean hitBeforeRead = false;
        if (!runtime.backpointersDisabled) {
            resolvedBackpointers.add(latestToken);
            System.out.printf("%%%%%%%% !Stream!View use %s 6\n", runtime.toString());
            ILogUnitEntry r = runtime.getAddressSpaceView().read(latestToken);
            long backPointer = latestToken;
            while (r.getResultType() != LogUnitReadResponseMsg.ReadResultType.EMPTY
                    && r.getBackpointerMap().containsKey(streamID)) {
                long prevRead = backPointer;
                backPointer = r.getBackpointerMap().get(streamID);
                if (! (backPointer < prevRead)) {
                    // throw new Exception("Backpointer error in stream {}: backPointer {} prevRead {}", streamID.toString(), backPointer, prevRead);
                    System.out.printf("************************** ERROR/TODO: Backpointer error in stream %s: backPointer %d prevRead %d\n", streamID.toString(), backPointer, prevRead);
                    return resolvedBackpointers;
                }
                log.trace("Read backPointer to {} at {}", backPointer, prevRead);
                System.out.printf("Read backPointer to %d at %d\n", backPointer, prevRead);
                if (backPointer == read) {
                    resolvedBackpointers.add(backPointer);
                    break;
                } else if (backPointer == -1L) {
                    log.trace("Hit stream start at {}, ending", prevRead);
                    hitStreamStart = true;
                    break;
                } else if (backPointer < read - 1) {
                    hitBeforeRead = true;
                    break;
                } else if (backPointer < getCurrentContext().logPointer.get()) {
                    hitBeforeRead = true;
                    break;
                } else {
                    resolvedBackpointers.add(backPointer);
                }

                // following backpointers...
                log.trace("Following backpointer to {}\n", backPointer);
                r = runtime.getAddressSpaceView().read(backPointer);
                log.trace("Following backpointer to %d\n", backPointer);
                System.out.printf("%%%%%%%% !Stream!View use %s 7, r = %s\n", runtime.toString(), r.toString());
                try {
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        } else {
            resolvedBackpointers.add(latestToken);
        }
        if (!hitStreamStart && !hitBeforeRead && resolvedBackpointers.first() != read) {
            long backpointerMin = resolvedBackpointers.first();
            log.trace("Backpointer min is at {} but read is at {}, filling.", backpointerMin, read);
            while (backpointerMin > read && backpointerMin > 0) {
                backpointerMin--;
                resolvedBackpointers.add(backpointerMin);
            }
        }
        log.trace("Backpointer resolved to {}.", resolvedBackpointers);
        return resolvedBackpointers;
    }

    /**
     * Read the next item from the stream.
     * This method is synchronized to protect against multiple readers.
     *
     * @return The next item from the stream.
     */
    @SuppressWarnings("unchecked")
    public synchronized ILogUnitEntry read() {
        while (true) {
            /*
            long thisRead = logPointer.getAndIncrement();
            if (thisRead == 0L)
            {
                //log.trace("Read[{}]: initial learn", streamID);
                //use backpointers to build
                //TODO: if this is a contiguous prefix, store in order to do a selective read.
                //runtime.getAddressSpaceView().readPrefix(streamID);
            }
            */

            // Pop the context if it has changed.
            if (getCurrentContext().logPointer.get() > getCurrentContext().maxAddress) {
                StreamContext last = streamContexts.pollFirst();
                log.trace("Completed context {}@{}, removing.", last.contextID, last.maxAddress);
            }

            Long thisRead = getCurrentContext().currentBackpointerList.pollFirst();
            if (thisRead == null) {
                getCurrentContext().currentBackpointerList =
                        resolveBackpointersToRead(
                                getCurrentContext().contextID,
                                getCurrentContext().logPointer.get());
                log.trace("Backpointer list was empty, it has been filled with {} entries.",
                        getCurrentContext().currentBackpointerList.size());
                if (getCurrentContext().currentBackpointerList.size() == 0) {
                    log.trace("No backpointers resolved, nothing to read.");
                    return null;
                }
                thisRead = getCurrentContext().currentBackpointerList.pollFirst();
            }

            getCurrentContext().logPointer.set(thisRead + 1);

            log.trace("Read[{}]: reading at {}", streamID, thisRead);
            System.out.printf("%%%%%%%% !Stream!View use %s 8\n", runtime.toString());
            ILogUnitEntry r = runtime.getAddressSpaceView().read(thisRead);
            if (r.getResultType() == LogUnitReadResponseMsg.ReadResultType.EMPTY) {
                //determine whether or not this is a hole
                System.out.printf("%%%%%%%% !Stream!View use %s 9\n", runtime.toString());
                long latestToken = runtime.getSequencerView().nextToken(Collections.singleton(streamID), 0).getToken();
                log.trace("Read[{}]: latest token at {}", streamID, latestToken);
                if (latestToken < thisRead) {
                    getCurrentContext().logPointer.decrementAndGet();
                    return null;
                }
                log.debug("Read[{}]: hole detected at {} (token at {}), attempting fill.", streamID, thisRead, latestToken);
                try {
                    System.out.printf("%%%%%%%% !Stream!View use %s 10\n", runtime.toString());
                    runtime.getAddressSpaceView().fillHole(thisRead);
                } catch (OverwriteException oe) {
                    //ignore overwrite.
                }
                System.out.printf("%%%%%%%% !Stream!View use %s 11\n", runtime.toString());
                r = runtime.getAddressSpaceView().read(thisRead);
                log.debug("Read[{}]: holeFill {} result: {}", streamID, thisRead, r.getResultType());
            }
            Set<UUID> streams = (Set<UUID>) r.getMetadataMap().get(IMetadata.LogUnitMetadataType.STREAM);
            if (streams != null && streams.contains(getCurrentContext().contextID)) {
                log.trace("Read[{}]: valid entry at {}", streamID, thisRead);
                Object res = r.getPayload();
                if (res instanceof StreamCOWEntry) {
                    StreamCOWEntry ce = (StreamCOWEntry) res;
                    log.trace("Read[{}]: encountered COW entry for {}@{}", streamID, ce.getOriginalStream(),
                            ce.getFollowUntil());
                    streamContexts.add(new StreamContext(ce.getOriginalStream(), ce.getFollowUntil()));
                } else {
                    return r;
                }
            }
        }
    }

    public synchronized ILogUnitEntry[] readTo(long pos) {
        long latestToken = pos;
        boolean max = false;
        if (pos == Long.MAX_VALUE) {
            max = true;
            System.out.printf("%%%%%%%% !Stream!View use %s 12\n", runtime.toString());
            latestToken = runtime.getSequencerView().nextToken(Collections.singleton(streamID), 0).getToken();
            log.trace("Linearization point set to {}", latestToken);
        }
        ArrayList<ILogUnitEntry> al = new ArrayList<>();
        log.debug("Stream[{}] pointer[{}], readTo {}", streamID, getCurrentContext().logPointer.get(), pos);
        while (getCurrentContext().logPointer.get() <= latestToken) {
            ILogUnitEntry r = read();
            if (r != null && (max || r.getAddress() <= pos)) {
                al.add(r);
            } else {
                break;
            }
        }
        return al.toArray(new ILogUnitEntry[al.size()]);
    }

    /**
     * Closes this resource, relinquishing any underlying resources.
     * This method is invoked automatically on object managed by the
     * {@code try}-with-resources statement.
     *
     * @throws Exception if this resource cannot be closed
     */
    @Override
    public void close() throws Exception {

    }

    @ToString
    class StreamContext implements Comparable<StreamContext> {

        /**
         * The id (stream ID) of this context.
         */
        final UUID contextID;

        /**
         * The maximum address that we should follow to, or
         * Long.MAX_VALUE, if this is the final context.
         */
        final long maxAddress;
        /**
         * A pointer to the log.
         */
        final AtomicLong logPointer;
        /**
         * A skipList of resolved stream addresses.
         */
        NavigableSet<Long> currentBackpointerList
                = new ConcurrentSkipListSet<>();

        public StreamContext(UUID contextID, long maxAddress) {
            this.contextID = contextID;
            this.maxAddress = maxAddress;
            this.logPointer = new AtomicLong(0);
        }

        @Override
        public int compareTo(StreamContext o) {
            return Long.compare(this.maxAddress, o.maxAddress);
        }
    }
}
