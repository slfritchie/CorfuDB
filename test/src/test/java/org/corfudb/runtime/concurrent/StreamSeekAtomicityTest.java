package org.corfudb.runtime.concurrent;

import lombok.extern.slf4j.Slf4j;

import org.corfudb.runtime.collections.SMRMap;
import org.corfudb.runtime.object.transactions.AbstractTransactionsTest;
import org.junit.Assert;
import org.junit.Test;
import org.corfudb.util.CoopScheduler;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.corfudb.util.CoopScheduler.*;

/**
 * Created by dmalkhi on 3/17/17.
 */
@Slf4j
public class StreamSeekAtomicityTest extends AbstractTransactionsTest {
    @Override
    public void TXBegin() { OptimisticTXBegin(); }

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
        ckCommitAtomicity(false);
    }

    /**
     * This test verifies commit atomicity against concurrent -read- activity,
     * which constantly causes rollbacks and optimistic-rollbacks.
     * Use the cooperative scheduler for thread interleaving control.
     *
     * @throws Exception
     */
    @Test
    public void ckCommitAtomicityCoop() throws Exception {
        ckCommitAtomicity(true);
    }

    public void ckCommitAtomicity(boolean useCoopSched) throws Exception {
        String mapName1 = "testMapA";
        Map<Long, Long> testMap1 = instantiateCorfuObject(SMRMap.class, mapName1);
        CountDownLatch l1 = new CountDownLatch(2);
        AtomicBoolean commitDone = new AtomicBoolean(false);
        final int NTHREADS = 3;
        int[] schedule = makeSchedule(NTHREADS, numIterations / innerIterations);

        scheduleConcurrently(t -> {

            int txCnt;
            for (txCnt = 0; txCnt < numIterations; txCnt++) {
                TXBegin();

                // on first iteration, wait for all to start;
                // other iterations will proceed right away
                l1.await();
                if (txCnt == 0) { coopRegisterThread(useCoopSched, 0); }

                // generate optimistic mutation
                sched(useCoopSched);
                testMap1.put(1L, (long)txCnt);

                // wait for it to be undon
                sched(useCoopSched);
                TXEnd();
            }
            // signal done
            commitDone.set(true);

            String failMsg = useCoopSched ? ("schedule was: " + formatSchedule(schedule) + "\n") : "";
            Assert.assertEquals(failMsg, (long)(txCnt-1), (long) testMap1.get(1L));
            coopThreadDone(useCoopSched);
        });

        // thread that keeps affecting optimistic-rollback of the above thread
        scheduleConcurrently(t -> {

            TXBegin();
            testMap1.get(1L);

            // signal that transaction has started and obtained a snapshot
            l1.countDown();
            coopRegisterThread(useCoopSched, 1);

            // keep accessing the snapshot, causing optimistic rollback

            while (!commitDone.get()){
                sched(useCoopSched);
                testMap1.get(1L);
            }
            coopThreadDone(useCoopSched);
        });

        // thread that keeps syncing with the tail of log
        scheduleConcurrently(t -> {

            // signal that thread has started
            l1.countDown();
            coopRegisterThread(useCoopSched, 2);

            // keep updating the in-memory proxy from the log
            while (!commitDone.get() ){
                sched(useCoopSched);
                testMap1.get(1L);
            }
            coopThreadDone(useCoopSched);
        });

        Thread coop = coopSetScheduleAndRun(useCoopSched, NTHREADS, NTHREADS, schedule);
        executeScheduled(NTHREADS, PARAMETERS.TIMEOUT_LONG);
        coopJoinScheduler(coop);
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
        ckCommitAtomicity2(false);
    }

    /**
     * This test is similar to above, but with multiple maps.
     * It verifies commit atomicity against concurrent -read- activity,
     * which constantly causes rollbacks and optimistic-rollbacks.
     * Use the cooperative scheduler for thread interleaving control.
     *
     * @throws Exception
     */
    @Test
    public void ckCommitAtomicity2Coop() throws Exception {
        ckCommitAtomicity2(true);
    }

    public void ckCommitAtomicity2(boolean useCoopSched) throws Exception {
        String mapName1 = "testMapA";
        Map<Long, Long> testMap1 = instantiateCorfuObject(SMRMap.class, mapName1);
        String mapName2 = "testMapB";
        Map<Long, Long> testMap2 = instantiateCorfuObject(SMRMap.class, mapName2);

        CountDownLatch l1 = new CountDownLatch(2);
        AtomicBoolean commitDone = new AtomicBoolean(false);
        final int NTHREADS = 3;
        int[] schedule = makeSchedule(NTHREADS, numIterations / innerIterations);

        scheduleConcurrently(t -> {

            int txCnt;
            for (txCnt = 0; txCnt < numIterations; txCnt++) {
                TXBegin();

                // on first iteration, wait for all to start;
                // other iterations will proceed right away
                l1.await();
                if (txCnt == 0) { coopRegisterThread(useCoopSched, 0); }

                // generate optimistic mutation
                sched(useCoopSched);
                testMap1.put(1L, (long)txCnt);
                if (txCnt % 2 == 0) {
                    sched(useCoopSched);
                    testMap2.put(1L, (long) txCnt);
                }

                // wait for it to be undone
                sched(useCoopSched);
                TXEnd();
            }
            // signal done
            commitDone.set(true);

            String failMsg = useCoopSched ? ("schedule was: " + formatSchedule(schedule) + "\n") : "";
            Assert.assertEquals(failMsg, (long)(txCnt-1), (long) testMap1.get(1L));
            Assert.assertEquals(failMsg, (long)( (txCnt-1) % 2 == 0 ? (txCnt-1) : txCnt-2), (long) testMap2.get(1L));
            coopThreadDone(useCoopSched);
        });

        // thread that keeps affecting optimistic-rollback of the above thread
        scheduleConcurrently(t -> {
            TXBegin();
            testMap1.get(1L);

            // signal that transaction has started and obtained a snapshot
            l1.countDown();
            coopRegisterThread(useCoopSched, 1);

            // keep accessing the snapshot, causing optimistic rollback

            while (!commitDone.get()){
                sched(useCoopSched);
                testMap1.get(1L);
                sched(useCoopSched);
                testMap2.get(1L);
            }
            coopThreadDone(useCoopSched);
        });

        // thread that keeps syncing with the tail of log
        scheduleConcurrently(t -> {
            // signal that thread has started
            l1.countDown();
            coopRegisterThread(useCoopSched, 2);

            // keep updating the in-memory proxy from the log
            while (!commitDone.get() ){
                sched(useCoopSched);
                testMap1.get(1L);
                sched(useCoopSched);
                testMap2.get(1L);
            }
            coopThreadDone(useCoopSched);
        });

        Thread coop = coopSetScheduleAndRun(useCoopSched, NTHREADS, NTHREADS, schedule);
        executeScheduled(NTHREADS, PARAMETERS.TIMEOUT_LONG);
        coopJoinScheduler(coop);
    }

    /**
     * This test is similar to above, but with concurrent -write- activity
     *
     * @throws Exception
     */
    @Test
    public void ckCommitAtomicity3() throws Exception {
        for (int i = 0; i < innerIterations; i++) {
            ckCommitAtomicity3(i, false);
        }
    }

    /**
     * This test is similar to above, but with concurrent -write- activity.
     * Use the cooperative scheduler for thread interleaving control.
     *
     * @throws Exception
     */
    @Test
    public void ckCommitAtomicity3Coop() throws Exception {
        for (int i = 0; i < innerIterations; i++) {
            ckCommitAtomicity3(i, true);
        }
    }

    public void ckCommitAtomicity3(int iter, boolean useCoopSched) throws Exception {
        String mapName1 = "testMapA" + iter;
        Map<Long, Long> testMap1 = instantiateCorfuObject(SMRMap.class, mapName1);
        String mapName2 = "testMapB" + iter;
        Map<Long, Long> testMap2 = instantiateCorfuObject(SMRMap.class, mapName2);

        CountDownLatch l1 = new CountDownLatch(2);
        AtomicBoolean commitDone = new AtomicBoolean(false);
        final int NTHREADS = 3;
        int[] schedule = makeSchedule(NTHREADS, numIterations / innerIterations);

        scheduleConcurrently(t -> {
            int txCnt;
            for (txCnt = 0; txCnt < numIterations / innerIterations; txCnt++) {
                TXBegin();

                // on first iteration, wait for all to start;
                // other iterations will proceed right away
                l1.await();
                if (txCnt == 0) { coopRegisterThread(useCoopSched, 0); }

                // generate optimistic mutation
                sched(useCoopSched);
                testMap1.put(1L, (long)txCnt);
                if (txCnt % 2 == 0) {
                    sched(useCoopSched);
                    testMap2.put(1L, (long) txCnt);
                }

                // wait for it to be undon
                sched(useCoopSched);
                TXEnd();
            }
            // signal done
            commitDone.set(true);

            String failMsg = useCoopSched ? ("schedule was: " + formatSchedule(schedule) + "\n") : "";
            Assert.assertEquals(failMsg, (long)(txCnt-1), (long) testMap1.get(1L));
            Assert.assertEquals(failMsg, (long)( (txCnt-1) % 2 == 0 ? (txCnt-1) : txCnt-2), (long) testMap2.get(1L));
            coopThreadDone(useCoopSched);
        });

        // thread that keeps affecting optimistic-rollback of the above thread
        scheduleConcurrently(t -> {
            long specialVal = numIterations + 1;

            TXBegin();
            testMap1.get(1L);

            // signal that transaction has started and obtained a snapshot
            l1.countDown();
            coopRegisterThread(useCoopSched, 1);

            // keep accessing the snapshot, causing optimistic rollback

            while (!commitDone.get()){
                sched(useCoopSched);
                testMap1.put(1L, specialVal);
                sched(useCoopSched);
                testMap2.put(1L, specialVal);
            }
            coopThreadDone(useCoopSched);
        });

        // thread that keeps syncing with the tail of log
        scheduleConcurrently(t -> {
            // signal that thread has started
            l1.countDown();
            coopRegisterThread(useCoopSched, 2);

            // keep updating the in-memory proxy from the log
            while (!commitDone.get() ){
                sched(useCoopSched);
                testMap1.get(1L);
                sched(useCoopSched);
                testMap2.get(1L);
            }
            coopThreadDone(useCoopSched);
        });

        Thread coop = coopSetScheduleAndRun(useCoopSched, NTHREADS, NTHREADS, schedule);
        executeScheduled(NTHREADS, PARAMETERS.TIMEOUT_LONG);
        coopJoinScheduler(coop);
    }

    private static void coopRegisterThread(boolean useCoopSched, int thrId) {
        if (useCoopSched) {
            CoopScheduler.registerThread(thrId);
        }
    }

    private static void coopThreadDone(boolean useCoopSched) {
        if (useCoopSched) {
            CoopScheduler.threadDone();
        }
    }

    private static Thread coopSetScheduleAndRun(boolean useCoopSched, int maxThreads, int nThreads, int[] schedule) {
        Thread coop;
        if (useCoopSched) {
            coop = new Thread(() -> CoopScheduler.runScheduler(nThreads));
            CoopScheduler.reset(maxThreads);
            CoopScheduler.setSchedule(schedule);
            coop = new Thread(() -> CoopScheduler.runScheduler(nThreads));
            coop.start();
        } else {
            coop = new Thread(() -> { /* Do nothing */} );
        }
        return coop;
    }

    private static void coopJoinScheduler(Thread thr) throws InterruptedException {
        thr.join();
    }
}
