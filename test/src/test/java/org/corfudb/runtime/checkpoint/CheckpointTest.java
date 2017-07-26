package org.corfudb.runtime.checkpoint;

import lombok.extern.slf4j.Slf4j;
import com.google.common.reflect.TypeToken;
import lombok.Getter;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.MultiCheckpointWriter;
import org.corfudb.runtime.collections.SMRMap;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.object.AbstractObjectTest;
import org.corfudb.runtime.object.transactions.TransactionType;
import org.corfudb.runtime.view.ObjectOpenOptions;
import org.corfudb.util.serializer.ISerializer;
import org.corfudb.util.serializer.Serializers;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by dmalkhi on 5/25/17.
 */
@Slf4j
public class CheckpointTest extends AbstractObjectTest {

    @Getter
    CorfuRuntime myRuntime = null;

    void setRuntime() {
        myRuntime = new CorfuRuntime(getDefaultConfigurationString()).connect();
    }

    Map<String, Long> instantiateMap(String mapName) {
        final byte serializerByte = (byte) 20;
        ISerializer serializer = new CPSerializer(serializerByte);
        Serializers.registerSerializer(serializer);
        return (SMRMap<String, Long>)
                instantiateCorfuObject(
                        getMyRuntime(),
                        new TypeToken<SMRMap<String, Long>>() {},
                        mapName,
                        serializer);
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

        m2A = instantiateMap(streamNameA);
        m2B = instantiateMap(streamNameB);
    }

    /**
     * checkpoint the maps
     */
    void mapCkpoint() throws Exception {
        CorfuRuntime currentRuntime = getMyRuntime();
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
        long checkpointAddress = -1;
        long lastCheckpointAddress = -1;
        CorfuRuntime currentRuntime = getMyRuntime();

        for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_VERY_LOW; i++) {
            try {
                MultiCheckpointWriter mcw1 = new MultiCheckpointWriter();
                mcw1.addMap((SMRMap) m2A);
                mcw1.addMap((SMRMap) m2B);
                checkpointAddress = mcw1.appendCheckpoints(currentRuntime, author);

                try {
                    Thread.sleep(PARAMETERS.TIMEOUT_SHORT.toMillis());
                } catch (InterruptedException ie) {
                    //
                }
                // Trim the log
                currentRuntime.getAddressSpaceView().prefixTrim(checkpointAddress - 1);
                currentRuntime.getAddressSpaceView().gc();
                currentRuntime.getAddressSpaceView().invalidateServerCaches();
                currentRuntime.getAddressSpaceView().invalidateClientCache();
                lastCheckpointAddress = checkpointAddress;
            } catch (TrimmedException te) {
                // shouldn't happen
                te.printStackTrace();
                throw te;
            } catch (ExecutionException ee) {
                // We are the only thread performing trimming, therefore any
                // prior trim must have been by our own action.  We assume
                // that we don't go backward, so checking for equality will
                // catch violations of that assumption.
                assertThat(checkpointAddress).isEqualTo(lastCheckpointAddress);
            }

        }
    }

    /**
     *  Start a fresh runtime and instantiate the maps.
     * This time the we check that the new map instances contains all values
     * @param mapSize
     * @param expectedFullsize
     */
    void validateMapRebuild(int mapSize, boolean expectedFullsize) {
        setRuntime();
        try {
            Map<String, Long> localm2A = instantiateMap(streamNameA);
            Map<String, Long> localm2B = instantiateMap(streamNameB);
            for (int i = 0; i < localm2A.size(); i++) {
                assertThat(localm2A.get(String.valueOf(i))).isEqualTo((long) i);
            }
            for (int i = 0; i < localm2B.size(); i++) {
                assertThat(localm2B.get(String.valueOf(i))).isEqualTo(0L);
            }
            if (expectedFullsize) {
                assertThat(localm2A.size()).isEqualTo(mapSize);
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
            try {
                m2A.put(String.valueOf(i), (long) i);
                m2B.put(String.valueOf(i), (long) 0);
            } catch (TrimmedException te) {
                // shouldn't happen
                te.printStackTrace();
                throw te;
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
    @Test
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
            validateMapRebuild(mapSize, false);
        });

        executeScheduled(PARAMETERS.CONCURRENCY_SOME, PARAMETERS.TIMEOUT_LONG);

        // finally, after all three threads finish, again we start a fresh runtime and instante the maps.
        // This time the we check that the new map instances contains all values
        validateMapRebuild(mapSize, true);
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
    @Test
    public void emptyCkpointTest() throws Exception {
        final int mapSize = 0;

        scheduleConcurrently(1, ignored_task_num -> {
            mapCkpoint();
        });

        // thread 2: repeat ITERATIONS_LOW times starting a fresh runtime, and instantiating the maps.
        // they should be empty.
        scheduleConcurrently(PARAMETERS.NUM_ITERATIONS_LOW, ignored_task_num -> {
            validateMapRebuild(mapSize, true);
        });

        executeScheduled(PARAMETERS.CONCURRENCY_SOME, PARAMETERS.TIMEOUT_LONG);

        // Finally, after the two threads finish, again we start a fresh runtime and instante the maps.
        // Then verify they are empty.
        validateMapRebuild(mapSize, true);
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
    @Test
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
            validateMapRebuild(mapSize, true);
        });

        executeScheduled(PARAMETERS.CONCURRENCY_SOME, PARAMETERS.TIMEOUT_LONG);

        // Finally, after all three threads finish, again we start a fresh runtime and instante the maps.
        // This time the we check that the new map instances contains all values
        validateMapRebuild(mapSize, true);
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
        final int mapSize = PARAMETERS.NUM_ITERATIONS_LOW;

        // thread 1: pupolates the maps with mapSize items
        scheduleConcurrently(1, ignored_task_num -> {
            populateMaps(mapSize);
        });

        // thread 2: periodic checkpoint of the maps, repeating ITERATIONS_VERY_LOW times,
        // and immediate prefix-trim of the log up to the checkpoint position
        scheduleConcurrently(1, ignored_task_num -> {
            mapCkpointAndTrim();
        });

        // thread 3: repeated ITERATION_LOW times starting a fresh runtime, and instantiating the maps.
        // they should rebuild from the latest checkpoint (if available).
        // performs some sanity checks on the map state
        scheduleConcurrently(PARAMETERS.NUM_ITERATIONS_LOW, ignored_task_num -> {
            validateMapRebuild(mapSize, false);
        });

        executeScheduled(PARAMETERS.CONCURRENCY_SOME, PARAMETERS.TIMEOUT_LONG);

        // finally, after all three threads finish, again we start a fresh runtime and instante the maps.
        // This time the we check that the new map instances contains all values
        validateMapRebuild(mapSize, true);
    }

    /**
     * This test verifies that a client that recovers a map from checkpoint,
     * but wants the map at a snapshot -earlier- than the checkpoint,
     * will get a transactionAbortException.
     * <p>
     * For an optimistic transaction -earlier- than the checkpoint,
     * we will also get a transactionAbortException.
     * <p>
     * It works as follows.
     * We build a map with one hundred entries [0, 1, 2, 3, ...., 99].
     * Then use thread #3 to set up our optimistic transaction.
     * <p>
     * Then, take a checkpoint of the map.
     * <p>
     * Then, we prefix-trim the log up to position 50.
     * <p>
     * Now, we start a new runtime and instantiate this map at 52.
     * It should abort the transaction because the checkpoint
     * (in a separate CP stream) is ignored and the regular stream's
     * backpointers lead to the trimmed region.
     * <p>
     * Finally, check thread #3's optimistic transaction.  We should
     * get a transactionAbortException.  If we retry a new transaction,
     * we'll see the full map.
     */
    @Test
    public void snapshotAbortOptimisticNoAbortTest() throws Exception {
        final int mapSize = PARAMETERS.NUM_ITERATIONS_LOW;
        final int trimPosition = mapSize / 2;
        final int snapshotPosition = trimPosition + 2;
        final CorfuRuntime t3Runtime = getMyRuntime();
        final Map<String, Long> t3Map = instantiateMap(streamNameA);

        // Set up optimistic checkpoint at this point in the log, prior to
        // the checkpoint's starting point.
        t(3, () -> {
            t3Map.put("extra value 1", 0L);

            t3Runtime.getObjectsView().TXBuild()
                    .setType(TransactionType.OPTIMISTIC)
                    .begin();

            assertThat(t3Map.get("no such key")).isNull();
        });

        // Use a different runtime for all other threads.
        setRuntime();

        t(1, () -> {
                    // first, populate the map
                    for (int i = 0; i < mapSize; i++) {
                        m2A.put(String.valueOf(i), (long) i);
                    }
                });

        t(1, () -> {
                    // now, take a checkpoint and perform a prefix-trim
                    MultiCheckpointWriter mcw1 = new MultiCheckpointWriter();
                    mcw1.addMap((SMRMap) m2A);
                    long checkpointAddress = mcw1.appendCheckpoints(getMyRuntime(), author);

                    // Trim the log
                    getMyRuntime().getAddressSpaceView().prefixTrim(trimPosition);
                    getMyRuntime().getAddressSpaceView().gc();
                    getMyRuntime().getAddressSpaceView().invalidateServerCaches();
                    getMyRuntime().getAddressSpaceView().invalidateClientCache();

                });

        AtomicBoolean txnAbortFlag = new AtomicBoolean(false);

        // start a new runtime
        t(2, () -> {
                    setRuntime();

                    Map<String, Long> localm2A = instantiateMap(streamNameA);

                    // start a snapshot TX at position snapshotPosition
                    getMyRuntime().getObjectsView().TXBuild()
                            .setType(TransactionType.SNAPSHOT)
                            .setSnapshot(snapshotPosition - 1)
                            .begin();

                    // finally, instantiate the map for the snapshot and assert the txn aborts
                    try {
                        localm2A.get(String.valueOf(0));
                    } catch (TransactionAbortedException te) {
                        // this is an expected behavior!
                        txnAbortFlag.set(true);
                    }
                });
        assertThat(txnAbortFlag.get()).isTrue();

        // Check t3's optimistic transaction, see txn abort.
        txnAbortFlag.set(false);
        t(3, () -> {
                    System.err.printf("*****************\n");
                    t3Runtime.getAddressSpaceView().invalidateServerCaches();
                    t3Runtime.getAddressSpaceView().invalidateClientCache();

                    try {
                        System.err.printf("t3Map = %s\n", t3Map);
                        t3Map.get(String.valueOf(0));
                    } catch (TransactionAbortedException te) {
                        txnAbortFlag.set(true);
                    } finally {
                        t3Runtime.getObjectsView().TXEnd();
                    }
                });
        assertThat(txnAbortFlag.get()).isTrue();

        // Try again with an optimistic txn and see the whole map
        txnAbortFlag.set(false);
        t(3, () -> {
            System.err.printf("***** 2 ************\n");
            t3Runtime.getObjectsView().TXBuild()
                    .setType(TransactionType.OPTIMISTIC)
                    .begin();

            try {
                assertThat(t3Map.get(String.valueOf(0))).isEqualTo(0L);
                assertThat(t3Map.size()).isEqualTo(mapSize + 1);
            } catch (TransactionAbortedException te) {
                txnAbortFlag.set(true);
            } finally {
                t3Runtime.getObjectsView().TXEnd();
            }
        });
        assertThat(txnAbortFlag.get()).isFalse();
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
    @Test
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

    @Test
    public void prefixTrimTwiceAtSameAddress() throws Exception {
        final int mapSize = 5;
        populateMaps(mapSize);

        // Trim again in exactly the same place shouldn't fail
        getMyRuntime().getAddressSpaceView().prefixTrim(2);
        getMyRuntime().getAddressSpaceView().prefixTrim(2);

        // GC twice at the same place should be fine.
        getMyRuntime().getAddressSpaceView().gc();
        getMyRuntime().getAddressSpaceView().gc();
    }

    @Test
    public void transactionalReadAfterCheckpoint() throws Exception {
        Map<String, String> testMap = getDefaultRuntime().getObjectsView().build()
                .setTypeToken(new TypeToken<SMRMap<String, String>>() {
                })
                .setStreamName("test")
                .open();

        Map<String, String> testMap2 = getDefaultRuntime().getObjectsView().build()
                .setTypeToken(new TypeToken<SMRMap<String, String>>() {
                })
                .setStreamName("test2")
                .open();

        // Place entries into the map
        testMap.put("a", "a");
        testMap.put("b", "a");
        testMap.put("c", "a");
        testMap2.put("a", "c");
        testMap2.put("b", "c");
        testMap2.put("c", "c");

        // Insert a checkpoint
        MultiCheckpointWriter mcw = new MultiCheckpointWriter();
        mcw.addMap((SMRMap) testMap);
        mcw.addMap((SMRMap) testMap2);
        long checkpointAddress = mcw.appendCheckpoints(getRuntime(), "author");


        // TX1: Move object to 1
        getRuntime().getObjectsView().TXBuild()
                .setType(TransactionType.SNAPSHOT)
                .setSnapshot(1)
                .begin();

        testMap.get("a");
        getRuntime().getObjectsView().TXEnd();


        // Trim the log
        getRuntime().getAddressSpaceView().prefixTrim(checkpointAddress - 1);
        getRuntime().getAddressSpaceView().gc();
        getRuntime().getAddressSpaceView().invalidateServerCaches();
        getRuntime().getAddressSpaceView().invalidateClientCache();

        // TX2: Read most recent state in TX
        getRuntime().getObjectsView().TXBegin();
        testMap.put("a", "b");
        getRuntime().getObjectsView().TXEnd();

    }

    @Test
    public void testSuccessiveCheckpointTrim() throws Exception {
        final int nCheckpoints = 2;
        final long ckpointGap = 5;

        Map<String, String> testMap = getDefaultRuntime().getObjectsView().build()
                .setTypeToken(new TypeToken<SMRMap<String, String>>() {})
                .setStreamName("test")
                .open();

        long checkpointAddress = -1;
        // generate two successive checkpoints
        for (int ckpoint = 0; ckpoint < nCheckpoints; ckpoint++) {
            // Place 3 entries into the map
            testMap.put("a", "a"+ckpoint);
            testMap.put("b", "b"+ckpoint);
            testMap.put("c", "c"+ckpoint);

            // Insert a checkpoint
            MultiCheckpointWriter mcw = new MultiCheckpointWriter();
            mcw.addMap((SMRMap) testMap);
            checkpointAddress = mcw.appendCheckpoints(getRuntime(), "author");
        }

        // Trim the log in between the checkpoints
        getRuntime().getAddressSpaceView().prefixTrim(checkpointAddress - ckpointGap);
        getRuntime().getAddressSpaceView().gc();
        getRuntime().getAddressSpaceView().invalidateServerCaches();
        getRuntime().getAddressSpaceView().invalidateClientCache();

        // Ok, get a new view of the map
        Map<String, String> newTestMap = getDefaultRuntime().getObjectsView().build()
                .setTypeToken(new TypeToken<SMRMap<String, String>>() {})
                .addOption(ObjectOpenOptions.NO_CACHE)
                .setStreamName("test")
                .open();

        // try to get a snapshot inside the gap
        getRuntime().getObjectsView()
                .TXBuild()
                .setType(TransactionType.SNAPSHOT)
                // fails Dahlia's original:  .setSnapshot(checkpointAddress-1)
                // passes Dahlia's original: .setSnapshot(checkpointAddress)
                .setSnapshot(checkpointAddress-1)
                .begin();

        // Reading an entry from scratch should be ok
        assertThat(newTestMap.get("a"))
                .isEqualTo("a"+(nCheckpoints-1));
    }
}
