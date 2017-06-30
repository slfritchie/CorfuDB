package org.corfudb.runtime.concurrent;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.collections.SMRMap;
import org.corfudb.runtime.object.transactions.AbstractTransactionsTest;
import org.corfudb.util.CoopScheduler;
import org.corfudb.util.CoopUtil;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.corfudb.util.CoopScheduler.sched;

/**
 * Created by dalia on 3/18/17.
 */
@Slf4j
public class MapsAsMQsTest extends AbstractTransactionsTest {
    @Override
    public void TXBegin() { OptimisticTXBegin(); }


    protected int numIterations = PARAMETERS.NUM_ITERATIONS_LOW;
    private String scheduleString;

    /**
     * This test verifies commit atomicity against concurrent -read- activity,
     * which constantly causes rollbacks and optimistic-rollbacks.
     *
     * @throws Exception
     */
    @Test
    public void useMapsAsMQs() throws Exception {
        for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_VERY_LOW; i++) {
            long start = System.currentTimeMillis();
            useMapsAsMQs(i);
            // System.err.printf("Iter %d -> %d msec\n", i, System.currentTimeMillis() - start);
        }
    }

    /**
     * Typical iteration time = 150 msec on MacBook,
     * occasional outliers at 2.5 - 3.5 seconds.
     */
    public void useMapsAsMQs(int iter) throws Exception {
        String mapName1 = "testMapA" + iter;
        Map<Long, Long> testMap1 = instantiateCorfuObject(SMRMap.class, mapName1);

        final int nThreads = 4;
        final int schedLength = 300;
        CoopUtil m = new CoopUtil();
        AtomicInteger barrier = new AtomicInteger(0);
        AtomicInteger lock = new AtomicInteger(0);
        AtomicInteger c1 = new AtomicInteger(0);
        AtomicInteger c2 = new AtomicInteger(0);

        CoopScheduler.reset(nThreads);
        int[] schedule = CoopScheduler.makeSchedule(nThreads, schedLength);
        CoopScheduler.setSchedule(schedule);
        scheduleString = "Schedule is: " + CoopScheduler.formatSchedule(schedule);

        // 1st thread: producer of new "trigger" values
        m.scheduleCoopConcurrently((thr, t) -> {

            // wait for other threads to start
            CoopUtil.barrierCountdown(barrier);
            CoopUtil.barrierAwait(barrier, nThreads);
            log.debug("all started");

            for (int i = 0; i < numIterations; i++) {

                try {
                    sched();
                    CoopUtil.lock(lock);

                    // place a value in the map
                    sched();
                    testMap1.put(1L, (long) i);
                    log.debug("- sending 1st trigger " + i);
                    CoopScheduler.appendLog("put " + i);

                    // await for the consumer condition to circulate back
                    CoopUtil.await(lock, c2);

                    log.debug("- sending 2nd trigger " + i);


                } finally {
                    sched();
                    CoopUtil.unlock(lock);
                }
            }
        });

        // 2nd thread: monitor map and wait for "trigger" values to show up, produce 1st signal
        m.scheduleCoopConcurrently((thr, t) -> {

            // signal start
            CoopUtil.barrierCountdown(barrier);
            CoopUtil.barrierAwait(barrier, nThreads);

            for (int i = 0; i < numIterations; i++) {
                sched();
                while (testMap1.get(1L) == null || testMap1.get(1L) != (long) i) {
                    log.debug( "- wait for 1st trigger " + i);
                    sched();
                }
                log.debug( "- received 1st trigger " + i);
                CoopScheduler.appendLog("1st trigger " + i);

                // 1st producer signal through lock
                try {
                    sched();
                    CoopUtil.lock(lock);

                    // 1st producer signal
                    sched();
                    c1.set(1);
                    CoopScheduler.appendLog("1st producer");
                } finally {
                    sched();
                    CoopUtil.unlock(lock);
                }
            }
        });

        // 3rd thread: monitor 1st producer condition and produce a second "trigger"
        m.scheduleCoopConcurrently((thr, t) -> {

            // signal start
            CoopUtil.barrierCountdown(barrier);
            CoopUtil.barrierAwait(barrier, nThreads);

            for (int i = 0; i < numIterations; i++) {
                try {
                    sched();
                    TXBegin();
                    sched();
                    CoopUtil.lock(lock);
                    sched();

                    // wait for 1st producer signal
                    CoopUtil.await(lock, c1);
                    log.debug( "- received 1st condition " + i);
                    CoopScheduler.appendLog("1st condition " + i);

                    // produce another trigger value
                    sched();
                    testMap1.put(2L, (long) i);
                    log.debug( "- sending 2nd trigger " + i);
                    CoopScheduler.appendLog("2nd trigger " + i);
                    sched();
                    TXEnd();
                } finally {
                    CoopUtil.unlock(lock);
                }
            }
        });

        // 4th thread: monitor map and wait for 2nd "trigger" values to show up, produce second signal
        m.scheduleCoopConcurrently((thr, t) -> {

            // signal start
            CoopUtil.barrierCountdown(barrier);
            CoopUtil.barrierAwait(barrier, nThreads);

            int busyDelay = 1; // millisecs

            for (int i = 0; i < numIterations; i++) {
                sched();
                while (testMap1.get(2L) == null || testMap1.get(2L) != (long) i) {
                    sched();
                }
                log.debug( "- received 2nd trigger " + i);
                CoopScheduler.appendLog("2nd trigger " + i);

                // 2nd producer signal through lock
                try {
                    sched();
                    CoopUtil.lock(lock);

                    // 2nd producer signal
                    sched();
                    c2.set(1);
                    log.debug( "- sending 2nd signal " + i);
                    CoopScheduler.appendLog("2nd signal " + i);
                } finally {
                    CoopUtil.unlock(lock);
                }
            }
        });

        m.executeScheduled();
    }

}
