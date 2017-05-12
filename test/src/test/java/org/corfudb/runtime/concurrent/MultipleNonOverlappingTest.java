package org.corfudb.runtime.concurrent;

import com.google.common.reflect.TypeToken;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.collections.SMRMap;
import org.corfudb.runtime.object.transactions.AbstractTransactionsTest;
import org.corfudb.util.CoopScheduler;
import org.junit.Assert;
import org.junit.Test;
import org.corfudb.runtime.object.TestClass;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Map;

import static org.corfudb.util.CoopScheduler.formatSchedule;
import static org.corfudb.util.CoopScheduler.makeSchedule;
import static org.corfudb.util.CoopScheduler.sched;

@Slf4j
public class MultipleNonOverlappingTest extends AbstractTransactionsTest {
    @Override
    public void TXBegin() { WWTXBegin(); }



    private String mapName1;
    private String mapName2;

    /**
     * High level:
     *
     * This test will create OBJECT_NUM of objects in an SMRMap. Each object's sum will be incremented by VAL until we
     * reach FINAL_SUM. At the end of this test we:
     *
     *   1) Check that there are OBJECT_NUM number of objects in the SMRMap.
     *   2) Ensure that each object's sum is equal to FINAL_SUM
     *
     * Details (the mechanism by which we increment the values in each object):
     *
     *   1) We create THREAD_NUM number of threads
     *   2) Each thread is given a non-overlapping range on which to increment objects' sum. Each thread will increment
     *      it only once by VAL.
     *   3) We spawn all threads and we ensure that each object in the map is incremented only once. We wait for them
     *      to finish.
     *   4) Repeat the above steps FINAL_SUM number of times.
     */
    @Test
    public void testStress() throws Exception {
        final int maxThreads = 5;
        final int schedLength = 100;
        final int runMillis = 4000;
        long start = System.currentTimeMillis();

        for (int i = 0; System.currentTimeMillis() - start < runMillis; i++) {
            int[] schedule = makeSchedule(maxThreads, schedLength);
            testStressInner(maxThreads, i, schedule);
        }
    }

    public void testStressInner(int threadNum, int version, int[] schedule) throws Exception {
        mapName1 = "testMapA" + version;
        Map<Long, Long> testMap = instantiateCorfuObject(SMRMap.class, mapName1);

        final int VAL = 1;

        // You can fine tune the below parameters. OBJECT_NUM has to be a multiple of threadNum.
        final int OBJECT_NUM = 20;

        final int FINAL_SUM = OBJECT_NUM;
        final int STEP = OBJECT_NUM / threadNum;

        // test all objects advance in lock-step to FINAL_SUM
        for (int i = 0; i < FINAL_SUM; i++) {
            for (int j = 0; j < OBJECT_NUM; j += STEP) {
                NonOverlappingWriter n = new NonOverlappingWriter(i + 1, j, j + STEP, VAL);
                scheduleConcurrently(t -> {
                    int tnum = CoopScheduler.registerThread();
                    if (tnum < 0) {
                        System.err.printf("Thread registration failed\n");
                        System.exit(1);
                    }
                    try {
                        n.dowork();
                    } catch (Exception e) {
                        System.err.printf("Oops, exception %s in dowork()\n", e.toString());
                        throw e;
                    }
                    // System.err.printf("Done %d\n", tnum);
                    CoopScheduler.threadDone();
                });
            }
            CoopScheduler.reset(threadNum);
            CoopScheduler.setSchedule(schedule);
            Thread coop = new Thread(() -> CoopScheduler.runScheduler(threadNum) );
            coop.start();
            executeScheduled(threadNum, PARAMETERS.TIMEOUT_NORMAL);
            coop.join();
        }

        String failMsg = "schedule was: " + formatSchedule(schedule) + "\n";
        Assert.assertEquals(failMsg, testMap.size(), FINAL_SUM);
        for (Long value : testMap.values()) {
            Assert.assertEquals(failMsg, (long) FINAL_SUM, (long) value);
        }

    }

    /**
     * Same as above, but two maps, not advancing at the same pace
     * @throws Exception
     */
     @Test

    public void testStress2() throws Exception {
         final int maxThreads = 5;
         final int schedLength = 100;
         final int runMillis = 4000;
         long start = System.currentTimeMillis();

         for (int i = 0; System.currentTimeMillis() - start < runMillis; i++) {
            int[] schedule = makeSchedule(maxThreads, schedLength);
            testStress2Inner(maxThreads, i, schedule);
        }
    }

    public void testStress2Inner(int threadNum, int version, int[] schedule) throws Exception {
        mapName1 = "testMapA" + version;
        Map<Long, Long> testMap1 = instantiateCorfuObject(SMRMap.class, mapName1);
        mapName2 = "testMapB" + version;
        Map<Long, Long> testMap2 = instantiateCorfuObject(SMRMap.class, mapName2);

        final int VAL = 1;

        // You can fine tune the below parameters. OBJECT_NUM has to be a multiple of threadNum.
        final int OBJECT_NUM = 20;

        final int FINAL_SUM1 = OBJECT_NUM;
        final int FINAL_SUM2 = OBJECT_NUM/2+1;
        final int STEP = OBJECT_NUM / threadNum;

        // test all objects advance in lock-step to FINAL_SUM
        for (int i = 0; i < FINAL_SUM1; i++) {

            for (int j = 0; j < OBJECT_NUM; j += STEP) {
                NonOverlappingWriter n = new NonOverlappingWriter(i + 1, j, j + STEP, VAL);
                final int thrId = j / STEP;
                    scheduleConcurrently(t -> {
                        int tnum = CoopScheduler.registerThread(thrId);
                        if (tnum < 0) {
                            System.err.printf("Thread registration failed\n");
                            System.exit(1);
                        }
                        try {
                            n.dowork2();
                        } catch (Exception e) {
                            System.err.printf("Oops, exception %s in dowork()\n", e.toString());
                            throw e;
                        }
                        // System.err.printf("Done %d\n", tnum);
                        CoopScheduler.threadDone();
                    });
            }
            CoopScheduler.reset(threadNum);
            CoopScheduler.setSchedule(schedule);
            Thread coop = new Thread(() -> CoopScheduler.runScheduler(threadNum) );
            coop.start();
            executeScheduled(threadNum, PARAMETERS.TIMEOUT_NORMAL);
            coop.join();
        }

        String failMsg = "schedule was: " + formatSchedule(schedule) + "\n";
        Assert.assertEquals(failMsg, testMap2.size(), FINAL_SUM1);
        for (long i = 0; i < OBJECT_NUM; i++) {
            log.debug("final testmap1.get({}) = {}", i, testMap1.get(i));
            log.debug("final testmap2.get({}) = {}", i, testMap2.get(i));
            if (i % 2 == 0)
                Assert.assertEquals(failMsg, (long)testMap2.get(i), (long) FINAL_SUM2);
            else
                Assert.assertEquals(failMsg, (long)testMap2.get(i), (long) FINAL_SUM1);
        }

    }


    public class NonOverlappingWriter {
        Map<Long, Long> testMap1 = instantiateCorfuObject(SMRMap.class, mapName1);
        Map<Long, Long> testMap2 = instantiateCorfuObject(SMRMap.class, mapName2);


        int start;
        int end;
        int val;
        int expectedSum;

        public NonOverlappingWriter(int expectedSum, int start, int end, int val) {
            this.expectedSum = expectedSum;
            this.start = start;
            this.end = end;
            this.val = val;
        }

        /**
         * Updates objects between index start and index end.
         */
        public void dowork() {
            for (int i = start; i < end; i++) {
                simpleCreateImpl(i, val);
            }
        }

        /**
         * Updates objects between index start and index end.
         */
        public void dowork2() {
            for (int i = start; i < end; i++) {
                duoCreateImpl(i, val);
            }
        }

        /**
         * Does the actual work.
         *
         * @param idx SMRMap key index to update
         * @param val how much to increment the value by.
         */
        private void simpleCreateImpl(long idx, long val) {
            sched();
            TXBegin();

            sched();
            if (!testMap1.containsKey(idx)) {
                if (expectedSum - 1 > 0)
                    log.debug("OBJ FAIL {} doesn't exist expected={}",
                            idx, expectedSum);
                log.debug("OBJ {} PUT {}", idx, val);
                sched();
                testMap1.put(idx, val);
            } else {
                log.debug("OBJ {} GET", idx);
                sched();
                Long value = testMap1.get(idx);
                if (value != (expectedSum - 1))
                    log.debug("OBJ FAIL {} value={} expected={}", idx, value,
                            expectedSum - 1);
                log.debug("OBJ {} PUT {}+{}", idx, value, val);
                sched();
                testMap1.put(idx, value + val);
            }
            sched();
            TXEnd();

        }

        /**
         * Does the actual work.
         *
         * @param idx SMRMap key index to update
         * @param val how much to increment the value by.
         */
        private void duoCreateImpl(long idx, long val) {
            sched();
            TXBegin();

            sched();
            if (!testMap1.containsKey(idx)) {
                if (expectedSum - 1 > 0)
                    log.debug("OBJ FAIL {} doesn't exist expected={}",
                            idx, expectedSum);
                log.debug("OBJ {} PUT {}", idx, val);
                sched();
                testMap1.put(idx, val);
                sched();
                testMap2.put(idx, val);
            } else {
                log.debug("OBJ {} GET", idx);
                sched();
                Long value = testMap1.get(idx);
                if (value != (expectedSum - 1))
                    log.debug("OBJ FAIL {} value={} expected={}", idx, value,
                            expectedSum - 1);
                log.debug("OBJ {} PUT {}+{}", idx, value, val);
                sched();
                testMap1.put(idx, value + val);

                // in map 2, on even rounds, do this on for every other entry
                log.debug("OBJ2 {} GET", idx);
                sched();
                Long value2 = testMap2.get(idx);
                if (idx % 2 == 0) {
                    if (value2 != (expectedSum/2))
                        log.debug("OBJ2 FAIL {} value={} expected={}", idx, value2,
                                expectedSum/2);
                    if (expectedSum % 2 == 0) {
                        log.debug("OBJ2 {} PUT {}+{}", idx, value2, val);
                        sched();
                        testMap2.put(idx, value2 + val);
                    }
                } else {
                    if (value2 != (expectedSum - 1))
                        log.debug("OBJ2 FAIL {} value={} expected={}", idx, value2,
                                expectedSum - 1);
                    log.debug("OBJ2 {} PUT {}+{}", idx, value2, val);
                    sched();
                    testMap2.put(idx, value2 + val);
                }
            }
            sched();
            TXEnd();
        }
    }

    /**
     * A helper function that ends a fransaciton.
     */
    protected void TXEnd() {
        getRuntime().getObjectsView().TXEnd();
    }

    protected <T> T instantiateCorfuObject(Class<T> tClass, String name) {

        // TODO: Does not work at the moment.
        // corfudb.runtime.exceptions.NoRollbackException: Can't roll back due to non-undoable exception
        // getRuntime().getParameters().setUndoDisabled(true).setOptimisticUndoDisabled(true);

        return (T) getRuntime()
                .getObjectsView()
                .build()
                .setStreamName(name)     // stream name
                .setType(tClass)        // object class backed by this stream\
                .open();                // instantiate the object!
    }

    @Test
    public void checkCommitNested()  throws Exception {
        ArrayList<TestClass> objects = new ArrayList<>();

        final int nObjects = 2;
        for (int i = 0; i < nObjects; i++)
            objects.add((TestClass) instantiateCorfuObject(
                    new TypeToken<TestClass>() {
                    }, "test stream" + i)
            );

        final int nestingDepth = 8;

        // first, nest empty transactions up to depth 'nestingDepth'
        for (int nestLevel = 0; nestLevel < nestingDepth; nestLevel++) {
            TXBegin();
        }

        // access the object, to cause the TX stack to be copied as
        // optimistic stream
        //objects.get(0).get();


        // commit some of the nested transactions
        // each committed TX first invokes mutators only, no accessors
        for (int nestLevel = 0; nestLevel < nestingDepth/2; nestLevel++) {
            objects.get(0).set(nestLevel);
            TXEnd();
        }

        // then, restablish nested transactions up to the same nesting level
        // each TX invokes mutators only, no accessors, on the second object
        for (int nestLevel = 0; nestLevel < nestingDepth/2; nestLevel++) {
            TXBegin();
            objects.get(1).set(2*nestLevel);
        }

        // finally, access the objects
        assertThat(objects.get(1).get())
                .isEqualTo((nestingDepth/2-1)*2);
        assertThat(objects.get(0).get())
                .isEqualTo(nestingDepth/2-1);

    }
}

