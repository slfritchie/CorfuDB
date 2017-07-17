package org.corfudb.runtime.concurrent;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.collections.SMRMap;
import org.corfudb.runtime.object.transactions.AbstractTransactionsTest;
import org.corfudb.util.CoopScheduler;
import org.corfudb.util.CoopUtil;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.util.CoopScheduler.sched;

/**
 * Created by dalia on 3/18/17.
 */
@Slf4j
public class MapsAsMQsTest extends AbstractTransactionsTest {
    @Override
    public void TXBegin() { OptimisticTXBegin(); }


    protected int numIterations = PARAMETERS.NUM_ITERATIONS_LOW;
    final int T0 = 0, T1 = 1, T2 = 2, T3 = 3;
    private int[] schedule = null;
    private String scheduleString;
    final private int numAwaitRetries = 10;

    public static void main(String[] argv) {
        try {
            MapsAsMQsTest t = new MapsAsMQsTest();
            t.useMapsAsMQs_lots(argv.length > 0 && argv[0].contentEquals("fixed"));
            System.exit(0);
        } catch (Exception e) {
            System.err.printf("ERROR: Caught exception %s at:\n", e);
            e.printStackTrace();
            System.exit(1);
        }
    }

    public void useMapsAsMQs_lots(boolean fixedSchedule) throws Exception {
        final int numThreads = 4;
        final int schedLength = 900;
        ArrayList<Object[]> logs = new ArrayList<>();
        final int numTests = 2000;

        for (int i = 0; i < numTests; i++) {
            //// System.err.printf("Iter %d, thread count = %d\n", i, Thread.getAllStackTraces().size());
            System.err.printf("%d,", i);

            // @After methods:
            cleanupBuffers();
            try { cleanupScheduledThreads(); } catch (Exception e) {};
            shutdownThreadingTest();
            cleanPerTestTempDir();

            // @Before methods:
            becomeCorfuApp();
            resetTests();
            clearTestStatus();
            setupScheduledThreads();
            resetThreadingTest();
            InitSM();

            // We run into deadlock problems with synchronized blocks inside
            // of Caffeine if we do not disable runtime's caches.
            getDefaultRuntime().setCacheDisabled(true);

            if (fixedSchedule) {
                // TODO investigate starvation pathology? {1,3,3,3,0,2,1,3,2,1,2,0,0,2,0,1,1,1,1,3,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0}
                // TODO investigate starvation pathology? {1,0,1,1,3,2,3,1,3,1,3,1,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3}
                // TODO investigate starvation pathology? {3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,0,1,3,2,1,0,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1}
                // TODO investigate starvation pathology? {1,0,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,0,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,0,1,3,2,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,0,3,1,3,3,3,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,0,3,0,1,0,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,0,1,2,0,0,0,2,2,0,3,1,3,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0}
                schedule = new /*pathology*/ int[]{T1,T0,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T0,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T0,T1,T3,T2,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T0,T3,T1,T3,T3,T3,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T1,T0,T3,T0,T1,T0,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T3,T0,T1,T2,T0,T0,T0,T2,T2,T0,T3,T1,T3,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0,T0};
            } else {
                schedule = CoopScheduler.makeSchedule(numThreads, schedLength);
            }
            scheduleString = "Schedule = " + CoopScheduler.formatSchedule(schedule);
            System.err.printf(scheduleString + "\n");

            useMapsAsMQs(i, false, true);
            logs.add(CoopScheduler.getLog());

            // printLog(logs.get(i));
        }
    }



    /**
     * This test verifies commit atomicity against concurrent -read- activity,
     * which constantly causes rollbacks and optimistic-rollbacks.
     *
     * @throws Exception
     */
    @Test
    public void useMapsAsMQs() throws Exception {
        for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_LOW; i++) {
            long start = System.currentTimeMillis();
            useMapsAsMQs(i, true, false);
            // System.err.printf("Iter %d -> %d msec\n", i, System.currentTimeMillis() - start);
        }
    }

    /**
     * Typical iteration time = 150 msec on MacBook,
     * occasional outliers at 2.5 - 3.5 seconds.
     */
    public void useMapsAsMQs(int iter, boolean makeSchedule, boolean exitOnError) throws Exception {
        String mapName1 = "testMapA" + iter;
        Map<Long, Long> testMap1 = instantiateCorfuObject(SMRMap.class, mapName1);

        final int nThreads = 4;
        final int schedLength = 300;
        CoopUtil m = new CoopUtil();
        AtomicInteger barrier = new AtomicInteger(0);
        AtomicInteger lock = new AtomicInteger(0);
        AtomicInteger c1 = new AtomicInteger(0);
        AtomicInteger c2 = new AtomicInteger(0);
        AtomicBoolean failed = new AtomicBoolean(false);

        CoopScheduler.reset(nThreads);
        if (makeSchedule) {
            schedule = CoopScheduler.makeSchedule(nThreads, schedLength);
            scheduleString = "Schedule = " + CoopScheduler.formatSchedule(schedule);
        }
        System.err.printf("SCHED: %s\n", scheduleString);
        CoopScheduler.setSchedule(schedule);

        // 1st thread: producer of new "trigger" values
        m.scheduleCoopConcurrently((thr, t) -> {
            sched();

            // wait for other threads to start
            CoopUtil.barrierCountdown(barrier);
            CoopUtil.barrierAwait(barrier, nThreads);
            log.debug("all started");

            for (int i = 0; i < numIterations; i++) {

                try {
                    CoopUtil.lock(lock);

                    // place a value in the map
                    testMap1.put(1L, (long) i);
                    log.debug("- sending 1st trigger " + i);
                    CoopScheduler.appendLog("put " + i);

                    // await for the consumer condition to circulate back
                    while (! CoopUtil.await(lock, c2, numAwaitRetries)) {
                        if (failed.get() == true) {
                            return;
                        }
                    }
                    log.debug("- sending 2nd trigger " + i);
                } finally {
                    sched();
                    CoopUtil.unlock(lock);
                }
            }
        });

        // 2nd thread: monitor map and wait for "trigger" values to show up, produce 1st signal
        m.scheduleCoopConcurrently((thr, t) -> {
            sched();

            // signal start
            CoopUtil.barrierCountdown(barrier);
            CoopUtil.barrierAwait(barrier, nThreads);

            for (int i = 0; i < numIterations; i++) {
                try {
                    while (failed.get() == false &&
                            (testMap1.get(1L) == null || testMap1.get(1L) != (long) i)) {
                        log.debug("- wait for 1st trigger " + i);
                        sched();
                    }
                    log.debug("- received 1st trigger " + i);
                    CoopScheduler.appendLog("1st trigger " + i);

                    // 1st producer signal through lock
                    try {
                        CoopUtil.lock(lock);

                        // 1st producer signal
                        c1.set(1);
                        CoopScheduler.appendLog("1st producer");
                    } finally {
                        sched();
                        CoopUtil.unlock(lock);
                    }
                } catch (Exception e) {
                    System.out.printf("Exception: %s\n", e);
                    failed.set(true);
                    break;
                }
            }
        });

        // 3rd thread: monitor 1st producer condition and produce a second "trigger"
        m.scheduleCoopConcurrently((thr, t) -> {
            sched();

            // signal start
            CoopUtil.barrierCountdown(barrier);
            CoopUtil.barrierAwait(barrier, nThreads);

            for (int i = 0; i < numIterations; i++) {
                try {
                    TXBegin();
                    CoopUtil.lock(lock);

                    // wait for 1st producer signal
                    while (! CoopUtil.await(lock, c1, numAwaitRetries)) {
                        if (failed.get() == true) {
                            return;
                        }
                    }
                    log.debug( "- received 1st condition " + i);
                    CoopScheduler.appendLog("1st condition " + i);

                    // produce another trigger value
                    testMap1.put(2L, (long) i);
                    log.debug( "- sending 2nd trigger " + i);
                    CoopScheduler.appendLog("2nd trigger " + i);
                    sched(); // NOTE: if this sched() is removed from the rest, NPE in VersionLockedObject won't happen.
                    TXEnd();
                } finally {
                    CoopUtil.unlock(lock);
                }
            }
        });

        // 4th thread: monitor map and wait for 2nd "trigger" values to show up, produce second signal
        m.scheduleCoopConcurrently((thr, t) -> {
            sched();

            // signal start
            CoopUtil.barrierCountdown(barrier);
            CoopUtil.barrierAwait(barrier, nThreads);

            int busyDelay = 1; // millisecs

            for (int i = 0; i < numIterations; i++) {
                try {
                    int c = 0;
                    while (testMap1.get(2L) == null || testMap1.get(2L) != (long) i) {
                        sched();
                        if (failed.get() == true) {
                            System.err.printf("\n\nBREAK\n");
                            return;
                        }
                    }
                    log.debug("- received 2nd trigger " + i);
                    CoopScheduler.appendLog("2nd trigger " + i);

                    // 2nd producer signal through lock
                    try {
                        CoopUtil.lock(lock);

                        // 2nd producer signal
                        c2.set(1);
                        log.debug( "- sending 2nd signal " + i);
                        CoopScheduler.appendLog("2nd signal " + i);
                    } finally {
                        CoopUtil.unlock(lock);
                    }
                } catch (Exception e) {
                    System.err.printf("Exception: %s\n", e);
                    System.err.printf("Stack: ");
                    e.printStackTrace();
                    failed.set(true);
                    break;
                }
            }
        });

        m.executeScheduled();
        if (exitOnError) {
            if (failed.get() == true) {
                System.err.printf("\n\nFAILED with: %s\n", scheduleString);
                System.exit(1);
            }
        } else {
            assertThat(failed.get()).isFalse();
        }
    }

}
