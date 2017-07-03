package org.corfudb.runtime.concurrent;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Expiry;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.Ticker;
import com.google.common.collect.Maps;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.SMRMap;
import org.corfudb.runtime.object.transactions.AbstractTransactionsTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.Nonnull;

/**
 * Created by dmalkhi on 3/17/17.
 */
@Slf4j
public class StreamSeekAtomicityTest extends AbstractTransactionsTest {
    @Override
    public void TXBegin() { OptimisticTXBegin(); }



    protected int numIterations = PARAMETERS.NUM_ITERATIONS_LOW;

    /**
     * This test verifies commit atomicity against concurrent -read- activity,
     * which constantly causes rollbacks and optimistic-rollbacks.
     *
     * @throws Exception
     */
    @Test
    public void ckCommitAtomicity() throws Exception {
        String mapName1 = "testMapA";
        Map<Long, Long> testMap1 = instantiateCorfuObject(SMRMap.class, mapName1);
        CountDownLatch l1 = new CountDownLatch(2);
        AtomicBoolean commitDone = new AtomicBoolean(false);
        final int NTHREADS = 3;

        scheduleConcurrently(t -> {

            int txCnt;
            for (txCnt = 0; txCnt < numIterations; txCnt++) {
                TXBegin();

                // on first iteration, wait for all to start;
                // other iterations will proceed right away
                l1.await();

                // generate optimistic mutation
                testMap1.put(1L, (long)txCnt);

                // wait for it to be undon
                TXEnd();
            }
            // signal done
            commitDone.set(true);

            Assert.assertEquals((long)(txCnt-1), (long) testMap1.get(1L));
        });

        // thread that keeps affecting optimistic-rollback of the above thread
        scheduleConcurrently(t -> {
            TXBegin();
            testMap1.get(1L);

            // signal that transaction has started and obtained a snapshot
            l1.countDown();

            // keep accessing the snapshot, causing optimistic rollback

            while (!commitDone.get()){
                testMap1.get(1L);
            }
        });

        // thread that keeps syncing with the tail of log
        scheduleConcurrently(t -> {
            // signal that thread has started
            l1.countDown();

            // keep updating the in-memory proxy from the log
            while (!commitDone.get() ){
                testMap1.get(1L);
            }
        });

        executeScheduled(NTHREADS, PARAMETERS.TIMEOUT_NORMAL);
    }

    /**
     * This test is similar to above, but with multiple maps.
     * It verifies commit atomicity against concurrent -read- activity,
     * which constantly causes rollbacks and optimistic-rollbacks.
     *
     * @throws Exception
     */
    @Test
    public void ckCommitAtomicity2() throws Exception {
        String mapName1 = "testMapA";
        Map<Long, Long> testMap1 = instantiateCorfuObject(SMRMap.class, mapName1);
        String mapName2 = "testMapB";
        Map<Long, Long> testMap2 = instantiateCorfuObject(SMRMap.class, mapName2);

        CountDownLatch l1 = new CountDownLatch(2);
        AtomicBoolean commitDone = new AtomicBoolean(false);
        final int NTHREADS = 3;

        scheduleConcurrently(t -> {

            int txCnt;
            for (txCnt = 0; txCnt < numIterations; txCnt++) {
                TXBegin();

                // on first iteration, wait for all to start;
                // other iterations will proceed right away
                l1.await();

                // generate optimistic mutation
                testMap1.put(1L, (long)txCnt);
                if (txCnt % 2 == 0)
                    testMap2.put(1L, (long)txCnt);

                // wait for it to be undon
                TXEnd();
            }
            // signal done
            commitDone.set(true);

            Assert.assertEquals((long)(txCnt-1), (long) testMap1.get(1L));
            Assert.assertEquals((long)( (txCnt-1) % 2 == 0 ? (txCnt-1) : txCnt-2), (long) testMap2.get(1L));
        });

        // thread that keeps affecting optimistic-rollback of the above thread
        scheduleConcurrently(t -> {
            TXBegin();
            testMap1.get(1L);

            // signal that transaction has started and obtained a snapshot
            l1.countDown();

            // keep accessing the snapshot, causing optimistic rollback

            while (!commitDone.get()){
                testMap1.get(1L);
                testMap2.get(1L);
            }
        });

        // thread that keeps syncing with the tail of log
        scheduleConcurrently(t -> {
            // signal that thread has started
            l1.countDown();

            // keep updating the in-memory proxy from the log
            while (!commitDone.get() ){
                testMap1.get(1L);
                testMap2.get(1L);
            }
        });

        executeScheduled(NTHREADS, PARAMETERS.TIMEOUT_NORMAL);
    }

    /**
     * This test is similar to above, but with concurrent -write- activity
     *
     * @throws Exception
     */
    @Test
    public void ckCommitAtomicity3() throws Exception {
        String mapName1 = "testMapA";
        Map<Long, Long> testMap1 = instantiateCorfuObject(SMRMap.class, mapName1);
        String mapName2 = "testMapB";
        Map<Long, Long> testMap2 = instantiateCorfuObject(SMRMap.class, mapName2);

        CountDownLatch l1 = new CountDownLatch(2);
        AtomicBoolean commitDone = new AtomicBoolean(false);
        final int NTHREADS = 3;

        scheduleConcurrently(t -> {

            int txCnt;
            for (txCnt = 0; txCnt < numIterations; txCnt++) {
                TXBegin();

                // on first iteration, wait for all to start;
                // other iterations will proceed right away
                l1.await();

                // generate optimistic mutation
                testMap1.put(1L, (long)txCnt);
                if (txCnt % 2 == 0)
                    testMap2.put(1L, (long)txCnt);

                // wait for it to be undon
                TXEnd();
            }
            // signal done
            commitDone.set(true);

            Assert.assertEquals((long)(txCnt-1), (long) testMap1.get(1L));
            Assert.assertEquals((long)( (txCnt-1) % 2 == 0 ? (txCnt-1) : txCnt-2), (long) testMap2.get(1L));
        });

        // thread that keeps affecting optimistic-rollback of the above thread
        scheduleConcurrently(t -> {
            long specialVal = numIterations + 1;

            TXBegin();
            testMap1.get(1L);

            // signal that transaction has started and obtained a snapshot
            l1.countDown();

            // keep accessing the snapshot, causing optimistic rollback

            while (!commitDone.get()){
                testMap1.put(1L, specialVal);
                testMap2.put(1L, specialVal);
            }
        });

        // thread that keeps syncing with the tail of log
        scheduleConcurrently(t -> {
            // signal that thread has started
            l1.countDown();

            // keep updating the in-memory proxy from the log
            while (!commitDone.get() ){
                testMap1.get(1L);
                testMap2.get(1L);
            }
        });

        executeScheduled(NTHREADS, PARAMETERS.TIMEOUT_NORMAL);
    }

    @Getter
    private long logicalTime = -1;
    // public long getLogicalTime() { System.err.printf("lt = %d\n", logicalTime); return logicalTime; }

    private class logicalTicker implements Ticker {
        @Override
        public long read() { /* System.err.printf("t=%d,", logicalTime); */ return logicalTime; }
    }

    @Test
    public void caffeineTest() {
        CorfuRuntime runtime = getRuntime();
        Ticker myTicker = new logicalTicker();
        final long weight = 100*8;

        final LoadingCache<Long, String> readCache = Caffeine.<Long, String>newBuilder()
                .<Long, String>weigher((k, v) -> v.length())
                .maximumWeight(weight)
                .expireAfter(new Expiry<Long, String>() {
                    public long expireAfterCreate(Long key, String graph, long currentTime) {
                        // Use wall clock time, rather than nanotime, if from an external resource
                        return key;
                    }
                    public long expireAfterUpdate(Long key, String graph,
                                                  long currentTime, long currentDuration) {
                        return currentDuration;
                    }
                    public long expireAfterRead(Long key, String graph,
                                                long currentTime, long currentDuration) {
                        return currentDuration;
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

    private @Nonnull String cacheFetch(long address) {
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

}
