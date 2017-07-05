package org.corfudb.runtime.view;

import com.codahale.metrics.Gauge;
import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Expiry;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.Ticker;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import lombok.extern.slf4j.Slf4j;

import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.IToken;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.LogUnitClient;
import org.corfudb.runtime.exceptions.OverwriteException;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.exceptions.WrongEpochException;
import org.corfudb.util.CFUtils;

import java.util.Comparator;



/**
 * A view of the address space implemented by Corfu.
 *
 * <p>Created by mwei on 12/10/15.</p>
 */
@Slf4j
public class AddressSpaceView extends AbstractView {

    /**
     * Log address where this client has witnessed a TrimmedException,
     * which we assume was due to an earlier prefixTrim() operation.
     */
    private long trimmedWitnessed = -1;

    private class trimmedWitnessedTicker implements Ticker {
        @Override
        public long read() {
            return trimmedWitnessed;
        }
    }

    /**
     * A cache for read results.
     */
    final LoadingCache<Long, ILogData> readCache = Caffeine.<Long, ILogData>newBuilder()
            .<Long, ILogData>weigher((k, v) -> v.getSizeEstimate())
            .maximumWeight(runtime.getMaxCacheSize())
            .expireAfter(new Expiry<Long, ILogData>() {
                // Use a policy of logical time, where the logical clock advances
                // each time that we witness a TrimmedException.  The "time" for
                // a cache entry's expiry is its log address.
                public long expireAfterCreate(Long key, ILogData l, long currentTime) {
                    return key;
                }
                public long expireAfterUpdate(Long key, ILogData l,
                                              long currentTime, long currentDuration) {
                    return key;
                }
                public long expireAfterRead(Long key, ILogData l,
                                            long currentTime, long currentDuration) {
                    return key;
                }
            })
            .ticker(new trimmedWitnessedTicker())
            .recordStats()
            .build(new CacheLoader<Long, ILogData>() {
                @Override
                public ILogData load(Long value) throws Exception {
                    return cacheFetch(value);
                }

                @Override
                public Map<Long, ILogData> loadAll(Iterable<? extends Long> keys) throws Exception {
                    return cacheFetch((Iterable<Long>) keys);
                }
            });

    /**
     * Constructor for the Address Space View.
     */
    public AddressSpaceView(@Nonnull final CorfuRuntime runtime) {
        super(runtime);

        final String pfx = String.format("%s0x%x.cache.", runtime.getMpASV(), this.hashCode());
        runtime.getMetrics().register(pfx + "cache-size",
                (Gauge<Long>) () -> readCache.estimatedSize());
        runtime.getMetrics().register(pfx + "evictions",
                (Gauge<Long>) () -> readCache.stats().evictionCount());
        runtime.getMetrics().register(pfx + "hit-rate",
                (Gauge<Double>) () -> readCache.stats().hitRate());
        runtime.getMetrics().register(pfx + "hits",
                (Gauge<Long>) () -> readCache.stats().hitCount());
        runtime.getMetrics().register(pfx + "misses",
                (Gauge<Long>) () -> readCache.stats().missCount());
    }

    /**
     * Reset all in-memory caches.
     */
    public void resetCaches() {
        readCache.invalidateAll();
    }

    /** Write the given log data using a token, returning
     * either when the write has been completed successfully,
     * or throwing an OverwriteException if another value
     * has been adopted, or a WrongEpochException if the
     * token epoch is invalid.
     *
     * @param token     The token to use for the write.
     * @param data      The data to write.
     * @throws OverwriteException   If the globalAddress given
     *                              by the token has adopted
     *                              another value.
     * @throws WrongEpochException  If the token epoch is invalid.
     */
    public void write(IToken token, Object data) throws OverwriteException {
        final ILogData ld = new LogData(DataType.DATA, data);

        layoutHelper(l -> {
            // Check if the token issued is in the same
            // epoch as the layout we are about to write
            // to.
            if (token.getEpoch() != l.getEpoch()) {
                throw new WrongEpochException(l.getEpoch());
            }

            // Set the data to use the token
            ld.useToken(token);

            // Do the write
            l.getReplicationMode(token.getTokenValue())
                        .getReplicationProtocol(runtime)
                        .write(l, ld);
            return null;
        });

        // Cache the successful write
        if (!runtime.isCacheDisabled()) {
            readCache.put(token.getTokenValue(), ld);
        }
    }

    /** Directly read from the log, returning any
     * committed value, or NULL, if no value has
     * been committed.
     *
     * @param address   The address to read from.
     * @return          Committed data stored in the
     *                  log, or NULL, if no value
     *                  has been committed.
     */
    public @Nullable ILogData peek(final long address) {
        return layoutHelper(l -> l.getReplicationMode(address)
                    .getReplicationProtocol(runtime)
                    .peek(l, address));
    }

    /**
     * Read the given object from an address and streams.
     *
     * @param address An address to read from.
     * @return A result, which be cached.
     */
    public @Nonnull ILogData read(long address) {
        if (address <= trimmedWitnessed) {
            System.err.printf("DEBUG TrimmedException via trimmedWitnessed\n");
            throw new TrimmedException();
        }
        if (!runtime.isCacheDisabled()) {
            ILogData data = readCache.get(address);
            if (data == null || data.getType() == DataType.EMPTY) {
                throw new RuntimeException("Unexpected return of empty data at address "
                        + address + " on read");
            } else if (data.isTrimmed()) {
                // TODO: SLF: We don't know this client's access pattern (forward or backward).
                // And we don't know where the prefix trim point is, so it could be possible
                // to have an access pattern that's going forward ... and if that pattern
                // triggers a full cache scan+eviction on each read() attempt, then we'd
                // do many of eviction scans for very little reward.
                //
                // Ah, we can use getTrimMark() to figure out where the global trim
                // mark is and then run a single eviction scan.
                //
                // Or use the logical time scheme and hope that Caffeine does the same
                // space reclamation as an eviction scan.
                //
                // {sigh}  Which to choose?
                System.err.printf("DEBUG TrimmedException\n");
                if (address > trimmedWitnessed) {
                    trimmedWitnessed = address;
                }
                throw new TrimmedException();
            }
            return data;
        }
        return fetch(address);
    }

    /**
     * Read the given object from a range of addresses.
     *
     * @param addresses An iterable with addresses to read from
     * @return A result, which be cached.
     */
    public Map<Long, ILogData> read(Iterable<Long> addresses) {
        if (!runtime.isCacheDisabled()) {
            return readCache.getAll(addresses);
        }
        return this.cacheFetch(addresses);
    }

    /**
     * Get the first address in the address space.
     */
    public long getTrimMark() {
        return layoutHelper(l -> {
            return l.segments.stream()
                    .flatMap(seg -> seg.getStripes().stream())
                    .flatMap(stripe -> stripe.getLogServers().stream())
                    .map(endpoint ->
                            runtime.getRouter(endpoint)
                                    .getClient(LogUnitClient.class))
                    .map(LogUnitClient::getTrimMark)
                    .map(CFUtils::getUninterruptibly)
                    .max(Comparator.naturalOrder()).get();
        });
    }

    /**
     * Prefix trim the address space.
     *
     * <p>At the end of a prefix trim, all addresses equal to or
     * less than the address given will be marked for trimming,
     * which means that they may return either the original
     * data, or a trimmed exception.</p>
     *
     * @param address log address
     */
    public void prefixTrim(final long address) {
        log.debug("PrefixTrim[{}]", address);
        layoutHelper(l -> {
                    l.getPrefixSegments(address).stream()
                            .flatMap(seg -> seg.getStripes().stream())
                            .flatMap(stripe -> stripe.getLogServers().stream())
                            .map(endpoint ->
                                    runtime.getRouter(endpoint)
                                            .getClient(LogUnitClient.class))
                            .map(client -> client.prefixTrim(address))
                            .forEach(CFUtils::getUninterruptibly);
                    return null;    // No return value
                }
        );
    }

    /** Force compaction on an address space, which will force
     * all log units to free space, and process any outstanding
     * trim requests.
     *
     */
    public void gc() {
        log.debug("GarbageCollect");
        layoutHelper(l -> {
            l.segments.stream()
                    .flatMap(seg -> seg.getStripes().stream())
                    .flatMap(stripe -> stripe.getLogServers().stream())
                    .map(endpoint ->
                            runtime.getRouter(endpoint)
                                    .getClient(LogUnitClient.class))
                    .map(LogUnitClient::compact)
                    .forEach(CFUtils::getUninterruptibly);
            return null;
        });
    }

    /** Force all server caches to be invalidated.
     */
    public void invalidateServerCaches() {
        log.debug("InvalidateServerCaches");
        layoutHelper(l -> {
            l.segments.stream()
                    .flatMap(seg -> seg.getStripes().stream())
                    .flatMap(stripe -> stripe.getLogServers().stream())
                    .map(endpoint ->
                            runtime.getRouter(endpoint)
                                    .getClient(LogUnitClient.class))
                    .map(LogUnitClient::flushCache)
                    .forEach(CFUtils::getUninterruptibly);
            return null;
        });
    }

    /** Force the client cache to be invalidated. */
    public void invalidateClientCache() {
        readCache.invalidateAll();
    }

    /**
     * Fetch an address for insertion into the cache.
     *
     * @param address An address to read from.
     * @return A result to be cached. If the readresult is empty,
     *         This entry will be scheduled to self invalidate.
     */
    private @Nonnull ILogData cacheFetch(long address) {
        log.trace("CacheMiss[{}]", address);
        ILogData result = fetch(address);
        if (result.getType() == DataType.EMPTY) {
            throw new RuntimeException("Unexpected empty return at " +  address + " from fetch");
        }
        return result;
    }

    /**
     * Fetch a collection of addresses for insertion into the cache.
     *
     * @param addresses collection of addresses to read from.
     * @return A result to be cached
     */
    public @Nonnull Map<Long, ILogData> cacheFetch(Iterable<Long> addresses) {
        //turn the addresses into Set for now to satisfy signature requirement down the line
        Set<Long> readAddresses = new TreeSet<>();
        Iterator<Long> iterator = addresses.iterator();
        while(iterator.hasNext()){
            readAddresses.add(iterator.next());
        }

        //doesn't handle the case where some address have a different replication mode
        return layoutHelper(l -> l.getReplicationMode(readAddresses.iterator().next())
                .getReplicationProtocol(runtime)
                .readAll(l, readAddresses));
    }


    /**
     * Explicitly fetch a given address, bypassing the cache.
     *
     * @param address An address to read from.
     * @return A result, which will be uncached.
     */
    public @Nonnull ILogData fetch(final long address) {
        return layoutHelper(l -> l.getReplicationMode(address)
                .getReplicationProtocol(runtime)
                .read(l, address)
        );
    }
}
