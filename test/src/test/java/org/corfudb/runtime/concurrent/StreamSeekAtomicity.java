package org.corfudb.runtime.concurrent;

import lombok.extern.slf4j.Slf4j;

import org.corfudb.runtime.collections.SMRMap;
import org.corfudb.runtime.object.transactions.AbstractObjectTest;
import org.corfudb.runtime.object.transactions.TransactionType;
import org.corfudb.util.CoopScheduler;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.corfudb.util.CoopScheduler.makeSchedule;
import static org.corfudb.util.CoopScheduler.printSchedule;
import static org.corfudb.util.CoopScheduler.sched;

/**
 * Created by dmalkhi on 3/17/17.
 */
@Slf4j
public class StreamSeekAtomicity extends AbstractObjectTest  {

    protected int numIterations = PARAMETERS.NUM_ITERATIONS_MODERATE;
    private final int innerIterations = 10;

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
        for (int i = 0; i < innerIterations; i++) {
            ckCommitAtomicity3Inner(i);
        }
    }

    public void ckCommitAtomicity3Inner(int iter) throws Exception {
        String mapName1 = "testMapA" + iter;
        Map<Long, Long> testMap1 = instantiateCorfuObject(SMRMap.class, mapName1);
        String mapName2 = "testMapB" + iter;
        Map<Long, Long> testMap2 = instantiateCorfuObject(SMRMap.class, mapName2);

        CountDownLatch l1 = new CountDownLatch(2);
        AtomicBoolean commitDone = new AtomicBoolean(false);
        final int NTHREADS = 3;

        int[] schedule = makeSchedule(NTHREADS, numIterations / innerIterations);
        // printSchedule(schedule);

        scheduleConcurrently(t -> {
            int txCnt;
            for (txCnt = 0; txCnt < numIterations / innerIterations; txCnt++) {
                TXBegin();

                // on first iteration, wait for all to start;
                // other iterations will proceed right away
                l1.await();
                if (txCnt == 0) { CoopScheduler.registerThread(); } // sched() attempts before now will have no effect

                // generate optimistic mutation
                sched(); testMap1.put(1L, (long)txCnt);
                if (txCnt % 2 == 0) {
                    sched(); testMap2.put(1L, (long) txCnt);
                }

                // wait for it to be undon
                sched(); TXEnd();
            }
            // signal done
            commitDone.set(true);

            Assert.assertEquals((long)(txCnt-1), (long) testMap1.get(1L));
            Assert.assertEquals((long)( (txCnt-1) % 2 == 0 ? (txCnt-1) : txCnt-2), (long) testMap2.get(1L));
            CoopScheduler.threadDone();
        });

        // thread that keeps affecting optimistic-rollback of the above thread
        scheduleConcurrently(t -> {
            long specialVal = numIterations + 1;

            TXBegin();
            testMap1.get(1L);

            // signal that transaction has started and obtained a snapshot
            l1.countDown();
            CoopScheduler.registerThread(); // sched() attempts before now will have no effect

            // keep accessing the snapshot, causing optimistic rollback

            while (!commitDone.get()){
                sched(); testMap1.put(1L, specialVal);
                sched(); testMap2.put(1L, specialVal);
            }
            CoopScheduler.threadDone();
        });

        // thread that keeps syncing with the tail of log
        scheduleConcurrently(t -> {
            // signal that thread has started
            l1.countDown();
            CoopScheduler.registerThread(); // sched() attempts before now will have no effect

            // keep updating the in-memory proxy from the log
            while (!commitDone.get() ){
                sched(); testMap1.get(1L);
                sched(); testMap2.get(1L);
            }
            CoopScheduler.threadDone();
        });

        CoopScheduler.reset(NTHREADS);
        CoopScheduler.setSchedule(schedule);
        Thread coop = new Thread(() -> CoopScheduler.runScheduler(NTHREADS) );
        coop.start();
        executeScheduled(NTHREADS, PARAMETERS.TIMEOUT_NORMAL);
        coop.join();
    }
}
