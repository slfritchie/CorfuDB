package org.corfudb.runtime.view;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Expiry;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.Ticker;

import lombok.Getter;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.view.stream.IStreamView;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.Nonnull;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Created by mwei on 1/8/16.
 */
public class StreamViewTest extends AbstractViewTest {

    @Getter
    final String defaultConfigurationString = getDefaultEndpoint();

    public CorfuRuntime r;

    @Before
    public void setRuntime() throws Exception {
        r = getDefaultRuntime().connect();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void canReadWriteFromStream()
            throws Exception {
        UUID streamA = UUID.nameUUIDFromBytes("stream A".getBytes());
        byte[] testPayload = "hello world".getBytes();

        IStreamView sv = r.getStreamsView().get(streamA);
        sv.append(testPayload);

        assertThat(sv.next().getPayload(getRuntime()))
                .isEqualTo("hello world".getBytes());

        assertThat(sv.next())
                .isEqualTo(null);
    }

    /**
     * Test that a client can call IStreamView.remainingUpTo after a prefix trim.
     * If remainingUpTo contains trimmed addresses, then they are ignored.
     */
    @Test
    public void testRemainingUpToWithTrim() {
        StreamOptions options = StreamOptions.builder()
                .ignoreTrimmed(true)
                .build();

        IStreamView txStream = runtime.getStreamsView().get(ObjectsView.TRANSACTION_STREAM_ID, options);
        final int firstIter = 50;
        for (int x = 0; x < firstIter; x++) {
            byte[] data = "Hello World!".getBytes();
            txStream.append(data);
        }

        List<ILogData> entries = txStream.remainingUpTo((firstIter - 1) / 2);
        assertThat(entries.size()).isEqualTo(firstIter / 2);

        runtime.getAddressSpaceView().prefixTrim((firstIter - 1) / 2);
        runtime.getAddressSpaceView().invalidateServerCaches();
        runtime.getAddressSpaceView().invalidateClientCache();

        entries = txStream.remainingUpTo((firstIter - 1) / 2);
        assertThat(entries.size()).isEqualTo(0);

        entries = txStream.remainingUpTo(firstIter);
        assertThat(entries.size()).isEqualTo((firstIter / 2));

        // Open the stream with a new client
        CorfuRuntime rt2 = new CorfuRuntime(getDefaultEndpoint()).connect();
        txStream = rt2.getStreamsView().get(ObjectsView.TRANSACTION_STREAM_ID, options);
        entries = txStream.remainingUpTo(Long.MAX_VALUE);
        assertThat(entries.size()).isEqualTo((firstIter / 2));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void canReadWriteFromStreamConcurrent()
            throws Exception {
        UUID streamA = UUID.nameUUIDFromBytes("stream A".getBytes());
        byte[] testPayload = "hello world".getBytes();

        IStreamView sv = r.getStreamsView().get(streamA);
        scheduleConcurrently(PARAMETERS.NUM_ITERATIONS_LOW,
                i -> sv.append(testPayload));
        executeScheduled(PARAMETERS.CONCURRENCY_SOME,
                PARAMETERS.TIMEOUT_NORMAL);

        scheduleConcurrently(PARAMETERS.NUM_ITERATIONS_LOW,
                i -> assertThat(sv.next().getPayload(getRuntime()))
                .isEqualTo("hello world".getBytes()));
        executeScheduled(PARAMETERS.CONCURRENCY_SOME,
                PARAMETERS.TIMEOUT_NORMAL);
        assertThat(sv.next())
                .isEqualTo(null);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void canReadWriteFromStreamWithoutBackpointers()
            throws Exception {
        r.setBackpointersDisabled(true);

        UUID streamA = UUID.nameUUIDFromBytes("stream A".getBytes());
        byte[] testPayload = "hello world".getBytes();

        IStreamView sv = r.getStreamsView().get(streamA);
        scheduleConcurrently(PARAMETERS.NUM_ITERATIONS_LOW, i ->
                sv.append(testPayload));
        executeScheduled(PARAMETERS.CONCURRENCY_SOME, PARAMETERS.TIMEOUT_NORMAL);

        scheduleConcurrently(PARAMETERS.NUM_ITERATIONS_LOW, i ->
                assertThat(sv.next().getPayload(getRuntime()))
                .isEqualTo("hello world".getBytes()));
        executeScheduled(PARAMETERS.CONCURRENCY_SOME, PARAMETERS.TIMEOUT_NORMAL);
        assertThat(sv.next())
                .isEqualTo(null);
    }


    @Test
    @SuppressWarnings("unchecked")
    public void canReadWriteFromCachedStream()
            throws Exception {
        CorfuRuntime r = getDefaultRuntime().connect()
                .setCacheDisabled(false);
        UUID streamA = UUID.nameUUIDFromBytes("stream A".getBytes());
        byte[] testPayload = "hello world".getBytes();

        IStreamView sv = r.getStreamsView().get(streamA);
        sv.append(testPayload);

        assertThat(sv.next().getPayload(getRuntime()))
                .isEqualTo("hello world".getBytes());

        assertThat(sv.next())
                .isEqualTo(null);
    }

    @Test
    public void canSeekOnStream()
        throws Exception
    {
        CorfuRuntime r = getDefaultRuntime().connect();
        IStreamView sv = r.getStreamsView().get(
                CorfuRuntime.getStreamID("stream  A"));

        // Append some entries
        sv.append("a".getBytes());
        sv.append("b".getBytes());
        sv.append("c".getBytes());

        // Try reading two entries
        assertThat(sv.next().getPayload(r))
                .isEqualTo("a".getBytes());
        assertThat(sv.next().getPayload(r))
                .isEqualTo("b".getBytes());

        // Seeking to the beginning
        sv.seek(0);
        assertThat(sv.next().getPayload(r))
                .isEqualTo("a".getBytes());

        // Seeking to the end
        sv.seek(2);
        assertThat(sv.next().getPayload(r))
                .isEqualTo("c".getBytes());

        // Seeking to the middle
        sv.seek(1);
        assertThat(sv.next().getPayload(r))
                .isEqualTo("b".getBytes());
    }

    @Test
    public void canFindInStream()
            throws Exception
    {
        CorfuRuntime r = getDefaultRuntime().connect();
        IStreamView svA = r.getStreamsView().get(
                CorfuRuntime.getStreamID("stream  A"));
        IStreamView svB = r.getStreamsView().get(
                CorfuRuntime.getStreamID("stream  B"));

        // Append some entries
        final long A_GLOBAL = 0;
        svA.append("a".getBytes());
        final long B_GLOBAL = 1;
        svB.append("b".getBytes());
        final long C_GLOBAL = 2;
        svA.append("c".getBytes());
        final long D_GLOBAL = 3;
        svB.append("d".getBytes());
        final long E_GLOBAL = 4;
        svA.append("e".getBytes());

        // See if we can find entries:
        // Should find entry "c"
        assertThat(svA.find(B_GLOBAL,
                IStreamView.SearchDirection.FORWARD))
                .isEqualTo(C_GLOBAL);
        // Should find entry "a"
        assertThat(svA.find(B_GLOBAL,
                IStreamView.SearchDirection.REVERSE))
                .isEqualTo(A_GLOBAL);
        // Should find entry "e"
        assertThat(svA.find(E_GLOBAL,
                IStreamView.SearchDirection.FORWARD_INCLUSIVE))
                .isEqualTo(E_GLOBAL);
        // Should find entry "c"
        assertThat(svA.find(C_GLOBAL,
                IStreamView.SearchDirection.REVERSE_INCLUSIVE))
                .isEqualTo(C_GLOBAL);

        // From existing to existing:
        // Should find entry "b"
        assertThat(svB.find(D_GLOBAL,
                IStreamView.SearchDirection.REVERSE))
                .isEqualTo(B_GLOBAL);
        // Should find entry "d"
        assertThat(svB.find(B_GLOBAL,
                IStreamView.SearchDirection.FORWARD))
                .isEqualTo(D_GLOBAL);

        // Bounds:
        assertThat(svB.find(D_GLOBAL,
                IStreamView.SearchDirection.FORWARD))
                .isEqualTo(Address.NOT_FOUND);
    }

    @Test
    public void canDoPreviousOnStream()
            throws Exception
    {
        CorfuRuntime r = getDefaultRuntime().connect();
        IStreamView sv = r.getStreamsView().get(
                CorfuRuntime.getStreamID("stream  A"));

        // Append some entries
        sv.append("a".getBytes());
        sv.append("b".getBytes());
        sv.append("c".getBytes());

        // Move backward should return null
        assertThat(sv.previous())
                .isNull();

        // Move forward
        sv.next(); // "a"
        sv.next(); // "b"

        // Should be now "a"
        assertThat(sv.previous().getPayload(r))
                .isEqualTo("a".getBytes());

        // Move forward, should be now "b"
        assertThat(sv.next().getPayload(r))
                .isEqualTo("b".getBytes());

        sv.next(); // "c"
        sv.next(); // null

        // Should be now "b"
        assertThat(sv.previous().getPayload(r))
                .isEqualTo("b".getBytes());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void streamCanSurviveOverwriteException()
            throws Exception {
        UUID streamA = CorfuRuntime.getStreamID("stream A");
        byte[] testPayload = "hello world".getBytes();

        // read from an address that hasn't been written to
        // causing a hole fill
        r.getAddressSpaceView().read(0L);

        // Write to the stream, and read back. The hole should be filled.
        IStreamView sv = r.getStreamsView().get(streamA);
        sv.append(testPayload);

        assertThat(sv.next().getPayload(getRuntime()))
                .isEqualTo("hello world".getBytes());

        assertThat(sv.next())
                .isEqualTo(null);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void streamWillHoleFill()
            throws Exception {
        //begin tests
        UUID streamA = CorfuRuntime.getStreamID("stream A");
        byte[] testPayload = "hello world".getBytes();

        // Generate a hole.
        r.getSequencerView().nextToken(Collections.singleton(streamA), 1);

        // Write to the stream, and read back. The hole should be filled.
        IStreamView sv = r.getStreamsView().get(streamA);
        sv.append(testPayload);

        assertThat(sv.next().getPayload(getRuntime()))
                .isEqualTo("hello world".getBytes());

        assertThat(sv.next())
                .isEqualTo(null);
    }


    @Test
    @SuppressWarnings("unchecked")
    public void streamWithHoleFill()
            throws Exception {
        UUID streamA = CorfuRuntime.getStreamID("stream A");

        byte[] testPayload = "hello world".getBytes();
        byte[] testPayload2 = "hello world2".getBytes();

        IStreamView sv = r.getStreamsView().get(streamA);
        sv.append(testPayload);

        //generate a stream hole
        TokenResponse tr =
                r.getSequencerView().nextToken(Collections.singleton(streamA), 1);

        // read from an address that hasn't been written to
        // causing a hole fill
        r.getAddressSpaceView().read(tr.getToken().getTokenValue());


        tr = r.getSequencerView().nextToken(Collections.singleton(streamA), 1);

        // read from an address that hasn't been written to
        // causing a hole fill
        r.getAddressSpaceView().read(tr.getToken().getTokenValue());


        sv.append(testPayload2);

        //make sure we can still read the stream.
        assertThat(sv.next().getPayload(getRuntime()))
                .isEqualTo(testPayload);

        assertThat(sv.next().getPayload(getRuntime()))
                .isEqualTo(testPayload2);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void prefixTrimThrowsException()
            throws Exception {
        //begin tests
        UUID streamA = CorfuRuntime.getStreamID("stream A");
        byte[] testPayload = "hello world".getBytes();

        // Write to the stream
        IStreamView sv = r.getStreamsView().get(streamA);
        sv.append(testPayload);

        // Trim the entry
        runtime.getAddressSpaceView().prefixTrim(0);
        runtime.getAddressSpaceView().gc();
        runtime.getAddressSpaceView().invalidateServerCaches();
        runtime.getAddressSpaceView().invalidateClientCache();

        // We should get a prefix trim exception when we try to read
        assertThatThrownBy(() -> sv.next())
                .isInstanceOf(TrimmedException.class);
    }


    @Getter
    private long logicalTime = -1;
    // public long getLogicalTime() { System.err.printf("lt = %d\n", logicalTime); return logicalTime; }

    private class logicalTicker implements Ticker {
        @Override
        public long read() { /* System.err.printf("t=%d,", logicalTime); */ return logicalTime; }
    }

    @Test
    @SuppressWarnings("checkstyle:magicnumber")
    public void caffeineExplorationTest() {
        CorfuRuntime runtime = getRuntime();
        Ticker myTicker = new logicalTicker();
        final long weight = 100*8;

        final LoadingCache<Long, String> readCache = Caffeine.<Long, String>newBuilder()
                .<Long, String>weigher((k, v) -> v.length())
                .maximumWeight(weight)
                .expireAfter(new Expiry<Long, String>() {
                    public long expireAfterCreate(Long key, String graph, long currentTime) {
                        return key;
                    }
                    public long expireAfterUpdate(Long key, String graph,
                                                  long currentTime, long currentDuration) {
                        return key;
                    }
                    public long expireAfterRead(Long key, String graph,
                                                long currentTime, long currentDuration) {
                        return key;
                    }
                })
                // .ticker(this::getLogicalTime) // Same behavior as .ticker(myTicker)
                .ticker(myTicker)
                .recordStats()
                .build(new CacheLoader<Long, String>() {
                    @Override
                    public String load(Long value) throws Exception {
                        return cacheFetch(value);
                    }

                    @Override
                    public Map<Long, String> loadAll(Iterable<? extends Long> keys) throws Exception {
                        return cacheFetch((Iterable<Long>) keys);
                    }
                });
        for (long i = 0; i < 150; i++) {
            String yo = readCache.get(i);
            // System.err.printf("i[%d] = %s\n", i, yo);
        }
        System.err.printf("Cache size = %d\n", readCache.estimatedSize());
        System.err.printf("Advance logical clock\n");
        logicalTime = 145;
        System.err.printf("Cache size = %d\n", readCache.estimatedSize());
        final long some = 150;
        for (int j = 0; j < 4; j++) {
            for (long i = some; i < some + (20); i++) {
                String yo = readCache.get(i);
                yo = readCache.get(4L);
                // System.err.printf("i[%d] = %s\n", i, yo);
                // System.err.printf("i[1] = %s\n", 1, readCache.get(1L));
            }
            System.err.printf("Cache size = %d\n", readCache.estimatedSize());
            try { Thread.sleep(1000); } catch (Exception e) {}
        }
        logicalTime = 165;
        System.err.printf("Advance logical clock to %d\n", logicalTime);
        for (int j = 0; j < 4; j++) {
            for (long i = some+20; i < some + (20) + 10; i++) {
                String yo = readCache.get(i);
                yo = readCache.get(4L);
                // System.err.printf("i[%d] = %s\n", i, yo);
                // System.err.printf("i[1] = %s\n", 1, readCache.get(1L));
            }
            System.err.printf("Cache size = %d\n", readCache.estimatedSize());
            try { Thread.sleep(1000); } catch (Exception e) {}
        }
        System.err.printf("before refresh, Cache size = %d\n", readCache.estimatedSize());
        readCache.asMap().keySet().stream().forEach(addr -> {
            if (addr <= logicalTime) { readCache.refresh(addr); }
        });
        System.err.printf("before invalidate, Cache size = %d\n", readCache.estimatedSize());
        System.err.printf("before invalidate, asMap size = %d\n", readCache.asMap().keySet().size());
        System.err.printf("before invalidate, asMap = "); readCache.asMap().keySet().stream().sorted().forEach(l -> System.err.printf("%d,", l)); System.err.printf("\n");
        System.err.printf("before invalidate, asMap WTF size = %d\n", readCache.asMap().keySet().size());
        System.err.printf("before invalidate, asMap WTF length B = %d\n", readCache.asMap().keySet().stream().sorted().toArray().length);
        AtomicInteger invalidated = new AtomicInteger(0);
        AtomicInteger skipped = new AtomicInteger(0);
        readCache.asMap().keySet().stream().forEach(addr -> {
            if (addr <= logicalTime) { readCache.invalidate(addr); invalidated.getAndIncrement(); }
            else { skipped.getAndIncrement(); }
        });
        System.err.printf("after invalidate, asMap size = %d\n", readCache.asMap().keySet().size());
        System.err.printf("after invalidate, asMap = "); readCache.asMap().keySet().stream().sorted().forEach(l -> System.err.printf("%d,", l)); System.err.printf("\n");
        System.err.printf("invalidated = %d, skipped = %d\n", invalidated.get(), skipped.get());
        for (int j = 0; j < 4; j++) {
            System.err.printf("Cache size = %d, stats = %s\n", readCache.estimatedSize(), readCache.stats());
        }
        System.err.printf("Get to 200\n");
        for (long i = 170; i < 200; i++) {
            String yo = readCache.get(i);
            // System.err.printf("i[%d] = %s\n", i, yo);
        }
        for (int j = 0; j < 4; j++) {
            System.err.printf("Cache size = %d\n", readCache.estimatedSize());
            try { Thread.sleep(1000); } catch (Exception e) {}
        }
        System.err.printf("asMap WTF length C = %d\n", readCache.asMap().keySet().stream().sorted().toArray().length);
        System.err.printf("Cache size = %d\n", readCache.estimatedSize());
    }

    private @Nonnull
    String cacheFetch(long address) {
        return "Hello " + address;
    }
    public @Nonnull Map<Long, String> cacheFetch(Iterable<Long> addresses) {
        Map<Long,String> res = new HashMap<>();
        Iterator<Long> iterator = addresses.iterator();
        while(iterator.hasNext()){
            Long i = iterator.next();
            res.put(i, cacheFetch(i));
        }
        return res;
    }

    @Test
    @SuppressWarnings("unchecked")
    public void trimCausesCacheEviction()
            throws Exception {
        UUID streamA = UUID.nameUUIDFromBytes("stream A".getBytes());
        UUID streamB = UUID.nameUUIDFromBytes("stream B".getBytes());
        byte[] testPayload = "hello world".getBytes();
        final int iters = 2;

        IStreamView svA = r.getStreamsView().get(streamA);
        IStreamView svB = r.getStreamsView().get(streamB);

        for (int i = 0; i < iters; i++) {
            svA.append(testPayload);
            svB.append(testPayload);
        }

        getRuntime().getAddressSpaceView().prefixTrim((iters*2)-1);
        getRuntime().getAddressSpaceView().invalidateClientCache();
        getRuntime().getAddressSpaceView().invalidateServerCaches();
        // Thread.sleep(1000);

        assertThatThrownBy(() -> svB.next().getPayload(getRuntime()))
                .isInstanceOf(TrimmedException.class);
        assertThatThrownBy(() -> svA.next().getPayload(getRuntime()))
                .isInstanceOf(TrimmedException.class);
    }
}
