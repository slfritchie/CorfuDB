package org.corfudb.runtime.checkpoint;

import lombok.extern.slf4j.Slf4j;
import com.google.common.reflect.TypeToken;
import lombok.Getter;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.MultiCheckpointWriter;
import org.corfudb.runtime.collections.SMRMap;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.exceptions.TrimmedUpcallException;
import org.corfudb.runtime.object.AbstractObjectTest;
import org.corfudb.runtime.object.transactions.TransactionType;
import org.corfudb.util.CoopScheduler;
import org.junit.Before;
import org.junit.Test;

import java.text.spi.CollatorProvider;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.util.CoopScheduler.sched;

/**
 * Created by dmalkhi on 5/25/17.
 */
@Slf4j
public class CheckpointTest extends AbstractObjectTest {

    @Getter
    CorfuRuntime myRuntime = null;

    void setRuntime() {
        myRuntime = new CorfuRuntime(getDefaultConfigurationString()).connect();
        // Deadlock prevention: Java 'synchronized' is used to lock the CorfuRuntime's
        // address space cache's ConcurrentHashMap.  From inside a 'synchronized' block,
        // computeIfAbsent can trigger a Corfu read which can be CoopSched + yield'ed,
        // causing deadlock.  Our workaround is to disable that cache.
        // TODO Figure out testing priority of runtime behavior + CoopSched with cache enabled.
        getMyRuntime().setCacheDisabled(true);
        getMyRuntime().getAddressSpaceView().invalidateServerCaches();
        getMyRuntime().getAddressSpaceView().invalidateClientCache();
    }

    Map<String, Long> instantiateMap(String mapName) {
        System.err.printf("\nI=%s@%s,\n", mapName, Thread.currentThread().getName());
        log.info("I={}@{}", mapName, Thread.currentThread().getName());
        return (SMRMap<String, Long>)
                instantiateCorfuObject(
                        getMyRuntime(),
                        new TypeToken<SMRMap<String, Long>>() {
                        },
                        mapName);
    }

    final String streamNameA = "mystreamA";
    final String streamNameB = "mystreamB";
    final String author = "ckpointTest";
    protected Map<String, Long> m2A;
    protected Map<String, Long> m2B;

    /**
     * common initialization for tests: establish Corfu runtime and instantiate two maps
     */
    @Before
    public void instantiateMaps() {

        myRuntime = getDefaultRuntime().connect();
        getMyRuntime().setCacheDisabled(true);
        myRuntime.setCacheDisabled(true);
        getMyRuntime().getAddressSpaceView().invalidateServerCaches();
        getMyRuntime().getAddressSpaceView().invalidateClientCache();

        /******
        m2A = instantiateMap(streamNameA);
        m2B = instantiateMap(streamNameB); ******/
        System.err.printf("\nNOTE: instantiateMaps doesn't actually instantiate anything here\n");
    }

    /**
     * checkpoint the maps
     */
    void mapCkpoint() throws Exception {
        CorfuRuntime currentRuntime = getMyRuntime();
        getMyRuntime().setCacheDisabled(true);
        getMyRuntime().getAddressSpaceView().invalidateServerCaches();
        getMyRuntime().getAddressSpaceView().invalidateClientCache();
        for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_VERY_LOW; i++) {
            MultiCheckpointWriter mcw1 = new MultiCheckpointWriter();
            mcw1.addMap((SMRMap) m2A);
            mcw1.addMap((SMRMap) m2B);
            long firstGlobalAddress1 = mcw1.appendCheckpoints(currentRuntime, author);
        }
    }

    /**
     * checkpoint the maps, and then trim the log
     */
    void mapCkpointAndTrim() throws Exception {
        CorfuRuntime currentRuntime = getMyRuntime();
        getMyRuntime().setCacheDisabled(true);
        getMyRuntime().getAddressSpaceView().invalidateServerCaches();
        getMyRuntime().getAddressSpaceView().invalidateClientCache();
        for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_VERY_LOW; i++) {
            try {
                MultiCheckpointWriter mcw1 = new MultiCheckpointWriter();
                mcw1.addMap((SMRMap) m2A);
                mcw1.addMap((SMRMap) m2B);
                sched();
                long checkpointAddress = mcw1.appendCheckpoints(currentRuntime, author);

                /***** SLF nothankyou:
                try {
                    Thread.sleep(PARAMETERS.TIMEOUT_SHORT.toMillis());
                } catch (InterruptedException ie) {
                    //
                } ******/
                // Trim the log
                sched();
                currentRuntime.getAddressSpaceView().prefixTrim(checkpointAddress - 1);
                sched();
                currentRuntime.getAddressSpaceView().gc();
                sched();
                currentRuntime.getAddressSpaceView().invalidateServerCaches();
                sched();
                currentRuntime.getAddressSpaceView().invalidateClientCache();
            } catch (TrimmedException te) {
                // shouldn't happen
                te.printStackTrace();
                throw te;
            }

        }
    }

    /**
     *  Start a fresh runtime and instantiate the maps.
     * This time the we check that the new map instances contains all values
     * @param mapSize
     * @param expectedFullsize
     */
    void validateMapRebuild(int mapSuffix, int mapSize, boolean expectedFullsize) {
        setRuntime();
        getMyRuntime().setCacheDisabled(true);
        getMyRuntime().getAddressSpaceView().invalidateServerCaches();
        getMyRuntime().getAddressSpaceView().invalidateClientCache();
        try {
            Map<String, Long> localm2A = instantiateMap(streamNameA + mapSuffix);
            Map<String, Long> localm2B = instantiateMap(streamNameB + mapSuffix);
            int max;
            max = localm2A.size();
            System.err.printf("\n%s@A%d,\n", Thread.currentThread().getName(), max);
            log.info("{}@A{}", Thread.currentThread().getName(), max);
            for (int i = 0; i < max; i++) {
                sched();
                assertThat(localm2A.get(String.valueOf(i))).isEqualTo((long) i);
            }
            max = localm2B.size();
            System.err.printf("\n%s@B%d,\n", Thread.currentThread().getName(), max);
            log.info("{}@B{}", Thread.currentThread().getName(), max);
            for (int i = 0; i < max; i++) {
                sched();
                assertThat(localm2B.get(String.valueOf(i))).isEqualTo(0L);
            }
            if (expectedFullsize) {
                sched();
                assertThat(localm2A.size()).isEqualTo(mapSize);
                sched();
                assertThat(localm2B.size()).isEqualTo(mapSize);
            }
        } catch (TrimmedException te) {
            // shouldn't happen
            te.printStackTrace();
            throw te;
        }
    }

    /**
     * initialize the two maps, the second one is all zeros
     * @param mapSize
     */
    void populateMaps(int mapSize) {
        for (int i = 0; i < mapSize; i++) {
            // If TrimmedUpcallException happens, we need to retry the put() calls,
            // because other parts of this test assume that the put() calls are
            // eventually successful.
            while (true) {
                try {
                    sched();
                    m2A.put(String.valueOf(i), (long) i);
                    sched();
                    m2B.put(String.valueOf(i), (long) 0);
                    System.err.printf("PUT%d,", i);
                    break;
                } catch (TrimmedUpcallException te) {
                    // NOTE: This is a possible exception nowadays.
                    // TODO: Get 100% deterministic replay when it does happen.
                    System.err.printf("NOTICE/TODO: %s\n", te);
                } catch (TrimmedException te) {
                    // shouldn't happen
                    te.printStackTrace();
                    throw te;
                }
            }
        }
    }

    /**
     * this test builds two maps, m2A m2B, and brings up three threads:
     * <p>
     * 1. one pupolates the maps with mapSize items
     * 2. one does a periodic checkpoint of the maps, repeating ITERATIONS_VERY_LOW times
     * 3. one repeatedly (LOW times) starts a fresh runtime, and instantiates the maps.
     * they should rebuild from the latest checkpoint (if available).
     * this thread performs some sanity checks on the map state
     * <p>
     * Finally, after all three threads finish, again we start a fresh runtime and instante the maps.
     * This time the we check that the new map instances contains all values
     *
     * @throws Exception
     */
    ////@Test
    public void periodicCkpointTest() throws Exception {
        final int mapSize = PARAMETERS.NUM_ITERATIONS_LOW;

        // thread 1: pupolates the maps with mapSize items
        scheduleConcurrently(1, ignored_task_num -> {
                    populateMaps(mapSize);
                });

        // thread 2: periodic checkpoint of the maps, repeating ITERATIONS_VERY_LOW times
        // thread 1: perform a periodic checkpoint of the maps, repeating ITERATIONS_VERY_LOW times
        scheduleConcurrently(1, ignored_task_num -> {
            mapCkpoint();
        });

        // thread 3: repeated ITERATION_LOW times starting a fresh runtime, and instantiating the maps.
        // they should rebuild from the latest checkpoint (if available).
        // performs some sanity checks on the map state
        scheduleConcurrently(PARAMETERS.NUM_ITERATIONS_LOW, ignored_task_num -> {
            validateMapRebuild(0, mapSize, false);
        });

        executeScheduled(PARAMETERS.CONCURRENCY_SOME, PARAMETERS.TIMEOUT_LONG);

        // finally, after all three threads finish, again we start a fresh runtime and instante the maps.
        // This time the we check that the new map instances contains all values
        validateMapRebuild(0, mapSize, true);
    }

    /**
     * this test builds two maps, m2A m2B, and brings up two threads:
     * <p>
     * 1. one thread performs ITERATIONS_VERY_LOW checkpoints
     * 2. one thread repeats ITERATIONS_LOW times starting a fresh runtime, and instantiating the maps.
     * they should be empty.
     * <p>
     * Finally, after the two threads finish, again we start a fresh runtime and instante the maps.
     * Then verify they are empty.
     *
     * @throws Exception
     */
    ////@Test
    public void emptyCkpointTest() throws Exception {
        final int mapSize = 0;

        scheduleConcurrently(1, ignored_task_num -> {
            mapCkpoint();
        });

        // thread 2: repeat ITERATIONS_LOW times starting a fresh runtime, and instantiating the maps.
        // they should be empty.
        scheduleConcurrently(PARAMETERS.NUM_ITERATIONS_LOW, ignored_task_num -> {
            validateMapRebuild(0, mapSize, true);
        });

        executeScheduled(PARAMETERS.CONCURRENCY_SOME, PARAMETERS.TIMEOUT_LONG);

        // Finally, after the two threads finish, again we start a fresh runtime and instante the maps.
        // Then verify they are empty.
        validateMapRebuild(0, mapSize, true);
    }

    /**
     * this test is similar to periodicCkpointTest(), but populating the maps is done BEFORE starting the checkpoint/recovery threads.
     * <p>
     * First, the test builds two maps, m2A m2B, and populates them with mapSize items.
     * <p>
     * Then, it brings up two threads:
     * <p>
     * 1. one does a periodic checkpoint of the maps, repeating ITERATIONS_VERY_LOW times
     * 2. one repeatedly (LOW times) starts a fresh runtime, and instantiates the maps.
     * they should rebuild from the latest checkpoint (if available).
     * this thread checks that all values are present in the maps
     * <p>
     * Finally, after all three threads finish, again we start a fresh runtime and instante the maps.
     * This time the we check that the new map instances contains all values
     *
     * @throws Exception
     */
    ////@Test
    public void periodicCkpointNoUpdatesTest() throws Exception {
        final int mapSize = PARAMETERS.NUM_ITERATIONS_LOW;

        // pre-populate map
        populateMaps(mapSize);

        // thread 1: perform a periodic checkpoint of the maps, repeating ITERATIONS_VERY_LOW times
        scheduleConcurrently(1, ignored_task_num -> {
            mapCkpoint();
        });

        // repeated ITERATIONS_LOW times starting a fresh runtime, and instantiating the maps.
        // they should rebuild from the latest checkpoint (if available).
        // this thread checks that all values are present in the maps
        scheduleConcurrently(PARAMETERS.NUM_ITERATIONS_LOW, ignored_task_num -> {
            validateMapRebuild(0, mapSize, true);
        });

        executeScheduled(PARAMETERS.CONCURRENCY_SOME, PARAMETERS.TIMEOUT_LONG);

        // Finally, after all three threads finish, again we start a fresh runtime and instante the maps.
        // This time the we check that the new map instances contains all values
        validateMapRebuild(0, mapSize, true);
    }

    /**
     * this test is similar to periodicCkpointTest(), but adds simultaneous log prefix-trimming.
     * <p>
     * the test builds two maps, m2A m2B, and brings up three threads:
     * <p>
     * 1. one pupolates the maps with mapSize items
     * 2. one does a periodic checkpoint of the maps, repeating ITERATIONS_VERY_LOW times,
     * and immediately trims the log up to the checkpoint position.
     * 3. one repeats ITERATIONS_LOW starting a fresh runtime, and instantiating the maps.
     * they should rebuild from the latest checkpoint (if available).
     * this thread performs some sanity checks on the map state
     * <p>
     * Finally, after all three threads finish, again we start a fresh runtime and instante the maps.
     * This time the we check that the new map instances contains all values
     *
     * @throws Exception
     */

    @Test
    public void periodicCkpointTrimTest() throws Exception {
        final int T0 = 0, T1 = 1, T2 = 2, T3 = 3, T4 = 4, T5 = 5, T6 = 6;
        int[] schedule = new int[]{T1, T1, T0, T2, T1, T1, T1, T0, T4, T3, T4, T3, T3, T3, T6, T5};
        int numThreads = T6+1;
        periodicCkpointTrimTestInner(0, schedule, numThreads);
    }

    // disable for now, revisit for 100% deterministic scheduling [sigh}
    @Test
    public void periodicCkpointTrimTest_50_percent_failure_usually() throws Exception {
        final int T6 = 6;
        int numThreads = T6 + 1;
        // WEIRD, this one always passes on iteration 0 and always fails on iteration 1.
        // Hmmmmmmmmm...........
        // int[] schedule = {3,0,3,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,0,6,2,3,0,4,5,6,4,2,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,0,0,4,0,5,4,4,1,0,6,6,3,3,3,3,3,0,0,6,4,0,2,5,5,5,5,5,5};
        int[] schedule = {1,5,5,5,4,6,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,0,5,4,1,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,0,0,4,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2};
        for (int i = 0; i < 2*2*2*2*2*2; i++) {
            System.err.printf("Iteration %d\n", i);
            periodicCkpointTrimTestInner(i, schedule, numThreads);
        }
    }

    @Test
    public void periodicCkpointTrimTest2() throws Exception {
        final int T6 = 6;
        int numThreads = T6+1;

        for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_LOW; i++) {
            int[] schedule = CoopScheduler.makeSchedule(numThreads, 100);
            CoopScheduler.printSchedule(schedule);
            periodicCkpointTrimTestInner(i, schedule, numThreads);
        }
    }

    public void periodicCkpointTrimTestInner(int mapSuffix, int[] schedule, int numThreads) throws Exception {
        final int mapSize = PARAMETERS.NUM_ITERATIONS_LOW;
        Thread ts[] = new Thread[numThreads];
        int idxTs = 0;
        AtomicBoolean workerThreadFailure = new AtomicBoolean(false);

        // Deadlock prevention: Java 'synchronized' is used to lock the CorfuRuntime's
        // address space cache's ConcurrentHashMap.  From inside a 'synchronized' block,
        // computeIfAbsent can trigger a Corfu read which can be CoopSched + yield'ed,
        // causing deadlock.  Our workaround is to disable that cache.
        // TODO Figure out testing priority of runtime behavior + CoopSched with cache enabled.
        getMyRuntime().setCacheDisabled(true);
        getMyRuntime().getAddressSpaceView().invalidateServerCaches();
        getMyRuntime().getAddressSpaceView().invalidateClientCache();
        m2A = instantiateMap(streamNameA + mapSuffix);
        m2B = instantiateMap(streamNameB + mapSuffix);

        CoopScheduler.reset(numThreads);
        CoopScheduler.setSchedule(schedule);
        assertThat(PARAMETERS.CONCURRENCY_SOME + 2).isEqualTo(numThreads);

        // thread 1: pupolates the maps with mapSize items
        ts[idxTs++] = new Thread(() -> {
            Thread.currentThread().setName("thr-0");
                CoopScheduler.registerThread(); sched();
                populateMaps(mapSize);
                CoopScheduler.threadDone();
        });

        // thread 2: periodic checkpoint of the maps, repeating ITERATIONS_VERY_LOW times,
        // and immediate prefix-trim of the log up to the checkpoint position
        ts[idxTs++] = new Thread(() -> {
            Thread.currentThread().setName("thr-1");
            CoopScheduler.registerThread(); sched();
            try {
                mapCkpointAndTrim();
            } catch (TransactionAbortedException e) {
                System.err.printf(",Hey, check this schedule!,");
                // Abort is possible, but no need to fail test if it does.
            } catch (Exception e) {
                workerThreadFailure.set(true);
            }
            CoopScheduler.threadDone();
        });

        // thread 3: repeated ITERATION_LOW times starting a fresh runtime, and instantiating the maps.
        // they should rebuild from the latest checkpoint (if available).
        // performs some sanity checks on the map state
        for (int i = 0; i < PARAMETERS.CONCURRENCY_SOME; i++) {
            final int ii = i;
            ts[idxTs++] = new Thread(() -> {
                Thread.currentThread().setName("thr-" + (ii + 2));
                System.err.printf("thr-%d\n", (ii + 2));
                CoopScheduler.registerThread(); try{Thread.sleep(50);}catch(Exception e){}; sched();
                validateMapRebuild(mapSuffix, mapSize, false);
                CoopScheduler.threadDone();
            });
        }
        for (int i = 0; i < ts.length; i++) { ts[i].start(); }
        // System.err.printf("Run scheduler\n");
        CoopScheduler.runScheduler(ts.length);
        // System.err.printf("Scheduler done\n");
        for (int i = 0; i < ts.length; i++) { ts[i].join(); }
        // System.err.printf("Join done\n");

        assertThat(workerThreadFailure.get()).isFalse();

        // finally, after all three threads finish, again we start a fresh runtime and instante the maps.
        // This time the we check that the new map instances contains all values
        validateMapRebuild(mapSuffix, mapSize, true);
        System.err.printf("\n");
    }

    /**
     * This test verifies that a client that recovers a map from checkpoint,
     * but wants the map at a snapshot -earlier- than the snapshot,
     * will either get a transactionAbortException, or get the right version of
     * the map.
     * <p>
     * It works as follows.
     * We build a map with one hundred entries [0, 1, 2, 3, ...., 99].
     * <p>
     * Note that, each entry is one put, so if a TX starts at snapshot at 77, it should see a map with 77 items 0, 1, 2, ..., 76.
     * We are going to verify that this works even if we checkpoint the map, and trim a prefix, say of the first 50 put's.
     * <p>
     * First, we then take a checkpoint of the map.
     * <p>
     * Then, we prefix-trim the log up to position 50.
     * <p>
     * Now, we start a new runtime and instantiate this map. It should build the map from a snapshot.
     * <p>
     * Finally, we start a snapshot-TX at timestamp 77. We verify that the map state is [0, 1, 2, 3, ..., 76].
     */
    ////@Test
    public void undoCkpointTest() throws Exception {
        final int mapSize = PARAMETERS.NUM_ITERATIONS_LOW;
        final int trimPosition = mapSize / 2;
        final int snapshotPosition = trimPosition + 2;

        t(1, () -> {

                    // first, populate the map
                    for (int i = 0; i < mapSize; i++) {
                        m2A.put(String.valueOf(i), (long) i);
                    }

                    // now, take a checkpoint and perform a prefix-trim
                    MultiCheckpointWriter mcw1 = new MultiCheckpointWriter();
                    mcw1.addMap((SMRMap) m2A);
                    long checkpointAddress = mcw1.appendCheckpoints(getMyRuntime(), author);

                    // Trim the log
                    getMyRuntime().getAddressSpaceView().prefixTrim(trimPosition);
                    getMyRuntime().getAddressSpaceView().gc();
                    getMyRuntime().getAddressSpaceView().invalidateServerCaches();
                    getMyRuntime().getAddressSpaceView().invalidateClientCache();

                }
        );

        AtomicBoolean trimExceptionFlag = new AtomicBoolean(false);

        // start a new runtime
        t(2, () -> {
                    setRuntime();

                    Map<String, Long> localm2A = instantiateMap(streamNameA);

                    // start a snapshot TX at position snapshotPosition
                    getMyRuntime().getObjectsView().TXBuild()
                            .setType(TransactionType.SNAPSHOT)
                            .setSnapshot(snapshotPosition - 1)
                            .begin();

                    // finally, instantiate the map for the snapshot and assert is has the right state
                    try {
                        localm2A.get(0);
                    } catch (TransactionAbortedException te) {
                        // this is an expected behavior!
                        trimExceptionFlag.set(true);
                    }

                    if (trimExceptionFlag.get() == false) {
                        assertThat(localm2A.size())
                                .isEqualTo(snapshotPosition);

                        // check map positions 0..(snapshot-1)
                        for (int i = 0; i < snapshotPosition; i++) {
                            assertThat(localm2A.get(String.valueOf(i)))
                                    .isEqualTo((long) i);
                        }

                        // check map positions snapshot..(mapSize-1)
                        for (int i = snapshotPosition; i < mapSize; i++) {
                            assertThat(localm2A.get(String.valueOf(i)))
                                    .isEqualTo(null);
                        }
                    }
                }
        );

    }

    /**
     * This test intentionally "delays" a checkpoint, to allow additional
     * updates to be appended to the stream after.
     * <p>
     * It works as follows. First, a transcation is started in order to set a
     * snapshot time.
     * <p>
     * Then, some updates are appended.
     * <p>
     * Finally, we take checkpoints. Since checkpoints occur within
     * transactions, they will be nested inside the outermost transaction.
     * Therefore, they will inherit their snapshot time from the outermost TX.
     *
     * @throws Exception
     */
    ////@Test
    public void delayedCkpointTest() throws Exception {
        final int mapSize = PARAMETERS.NUM_ITERATIONS_LOW;
        final int additional = mapSize / 2;

        // first, populate the map
        for (int i = 0; i < mapSize; i++) {
            m2A.put(String.valueOf(i), (long) i);
        }

        // in one thread, start a snapshot transaction and leave it open
        t(1, () -> {
            // start a snapshot TX at position snapshotPosition
            getMyRuntime().getObjectsView().TXBuild()
                    .setType(TransactionType.SNAPSHOT)
//                    .setForceSnapshot(false) // force snapshot when nesting
                    .setSnapshot(mapSize - 1)
                    .begin();
                }
        );

        // now delay
        // in another thread, introduce new updates to the map
        t(2, () -> {
            for (int i = 0; i < additional; i++) {
                        m2A.put(String.valueOf(mapSize+i), (long) (mapSize+i));
                    }
                }
        );

        // back in the first thread, checkpoint and trim
        t(1, () -> {
            // now, take a checkpoint and perform a prefix-trim
            MultiCheckpointWriter mcw1 = new MultiCheckpointWriter();
            mcw1.addMap((SMRMap) m2A);
            long checkpointAddress = mcw1.appendCheckpoints(getMyRuntime(), author);

            // Trim the log
            getMyRuntime().getAddressSpaceView().prefixTrim(checkpointAddress);
            getMyRuntime().getAddressSpaceView().gc();
            getMyRuntime().getAddressSpaceView().invalidateServerCaches();
            getMyRuntime().getAddressSpaceView().invalidateClientCache();

            getMyRuntime().getObjectsView().TXEnd();

        });

        // finally, verify that a thread can build the map correctly
        t(2, () -> {
            setRuntime();

            Map<String, Long> localm2A = instantiateMap(streamNameA);

            assertThat(localm2A.size())
                    .isEqualTo(mapSize+additional);
            for (int i = 0; i < mapSize; i++) {
                assertThat(localm2A.get(String.valueOf(i)))
                        .isEqualTo((long) i);
            }
            for (int i = mapSize; i < mapSize+additional; i++) {
                assertThat(localm2A.get(String.valueOf(i)))
                        .isEqualTo((long) i);
            }

        });
    }

}

