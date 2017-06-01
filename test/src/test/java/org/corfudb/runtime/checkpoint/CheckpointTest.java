package org.corfudb.runtime.checkpoint;

import lombok.extern.slf4j.Slf4j;
import com.google.common.reflect.TypeToken;
import lombok.Getter;
import org.apache.commons.lang.math.RandomUtils;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.MultiCheckpointWriter;
import org.corfudb.runtime.collections.SMRMap;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.object.AbstractObjectTest;
import org.corfudb.util.CoopScheduler;
import org.junit.Test;

import javax.swing.*;
import java.util.Map;
import java.util.Random;
import java.util.function.BiConsumer;

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
    }

    Map<String, Long> instantiateMap(String mapName) {
        return (SMRMap<String, Long>)
                instantiateCorfuObject(
                        getMyRuntime(),
                        new TypeToken<SMRMap<String, Long>>() {},
                        mapName);
    }

    @Test
    public void periodicCkpointTest() throws Exception {
        final String streamNameA = "mystreamA";
        final String streamNameB = "mystreamB";
        final String author = "periodicCkpoint";
        final int sizeAdjustment = 16; // size reduction to accomodate TRACE level debugging
        final int mapSize = PARAMETERS.NUM_ITERATIONS_MODERATE / sizeAdjustment;

        myRuntime = getDefaultRuntime().connect();

        Map<String, Long> m2A = instantiateMap(streamNameA);
        Map<String, Long> m2B = instantiateMap(streamNameB);

        scheduleConcurrently(1, ignored_task_num -> {
            for (int i = 0; i < mapSize; i++) {
                m2A.put(String.valueOf(i), (long)i);
                m2B.put(String.valueOf(i), (long)0);
            }
        });

        scheduleConcurrently(1, ignored_task_num -> {
            CorfuRuntime currentRuntime = getMyRuntime();
            for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_VERY_LOW; i++) {
                MultiCheckpointWriter mcw1 = new MultiCheckpointWriter();
                mcw1.addMap((SMRMap) m2A);
                mcw1.addMap((SMRMap) m2B);
                long firstGlobalAddress1 = mcw1.appendCheckpoints(currentRuntime, author);
            }
        });

        scheduleConcurrently(PARAMETERS.NUM_ITERATIONS_LOW, ignored_task_num -> {
            setRuntime();
            Map<String, Long> localm2A = instantiateMap(streamNameA);
            Map<String, Long> localm2B = instantiateMap(streamNameB);
            for (int i = 0; i < mapSize; i++) {
                assertThat(localm2A.get(String.valueOf(i)) == null ||
                        localm2A.get(String.valueOf(i)) == (long) i
                ).isTrue();
                assertThat(localm2B.get(String.valueOf(i)) == null ||
                        localm2B.get(String.valueOf(i)) == (long) 0
                ).isTrue();
            }
        });

        executeScheduled(PARAMETERS.CONCURRENCY_SOME, PARAMETERS.TIMEOUT_LONG);

        setRuntime();
        Map<String, Long> localm2A = instantiateMap(streamNameA);
        Map<String, Long> localm2B = instantiateMap(streamNameB);
        for (int i = 0; i < mapSize; i++) {
            assertThat(localm2A.get(String.valueOf(i)) ).isEqualTo((long)i);
            assertThat(localm2B.get(String.valueOf(i)) ).isEqualTo(0L);
        }

    }

    @Test
    public void emptyCkpointTest() throws Exception {
        final String streamNameA = "mystreamA";
        final String streamNameB = "mystreamB";
        final String author = "periodicCkpoint";
        final int sizeAdjustment = 16; // size reduction to accomodate TRACE level debugging
        final int mapSize = PARAMETERS.NUM_ITERATIONS_MODERATE / sizeAdjustment;

        myRuntime = getDefaultRuntime().connect();

        Map<String, Long> m2A = instantiateMap(streamNameA);
        Map<String, Long> m2B = instantiateMap(streamNameB);

        scheduleConcurrently(1, ignored_task_num -> {
            CorfuRuntime currentRuntime = getMyRuntime();
            for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_VERY_LOW; i++) {
                MultiCheckpointWriter mcw1 = new MultiCheckpointWriter();
                mcw1.addMap((SMRMap) m2A);
                mcw1.addMap((SMRMap) m2B);
                long firstGlobalAddress1 = mcw1.appendCheckpoints(currentRuntime, author);
            }
        });

        scheduleConcurrently(PARAMETERS.NUM_ITERATIONS_LOW, ignored_task_num -> {
            setRuntime();
            Map<String, Long> localm2A = instantiateMap(streamNameA);
            Map<String, Long> localm2B = instantiateMap(streamNameB);
            for (int i = 0; i < mapSize; i++) {
                assertThat(localm2A.get(String.valueOf(i)) ).isNull();
                assertThat(localm2B.get(String.valueOf(i)) ).isNull();
            }
        });

        executeScheduled(PARAMETERS.CONCURRENCY_SOME, PARAMETERS.TIMEOUT_LONG);

        setRuntime();
        Map<String, Long> localm2A = instantiateMap(streamNameA);
        Map<String, Long> localm2B = instantiateMap(streamNameB);
        for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_MODERATE; i++) {
            assertThat(localm2A.get(String.valueOf(i)) ).isNull();
            assertThat(localm2B.get(String.valueOf(i)) ).isNull();
        }

    }

    @Test
    public void periodicCkpointTestNoUpdates() throws Exception {
        final String streamNameA = "mystreamA";
        final String streamNameB = "mystreamB";
        final String author = "periodicCkpoint";
        final int sizeAdjustment = 16; // size reduction to accomodate TRACE level debugging
        final int mapSize = PARAMETERS.NUM_ITERATIONS_MODERATE / sizeAdjustment;

        myRuntime = getDefaultRuntime().connect();

        Map<String, Long> m2A = instantiateMap(streamNameA);
        Map<String, Long> m2B = instantiateMap(streamNameB);

        // pre-populate map
        for (int i = 0; i < mapSize; i++) {
            m2A.put(String.valueOf(i), (long)i);
            m2B.put(String.valueOf(i), (long)0);
        }

        scheduleConcurrently(1, ignored_task_num -> {
            CorfuRuntime currentRuntime = getMyRuntime();
            for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_VERY_LOW; i++) {
                MultiCheckpointWriter mcw1 = new MultiCheckpointWriter();
                mcw1.addMap((SMRMap) m2A);
                mcw1.addMap((SMRMap) m2B);
                mcw1.appendCheckpoints(currentRuntime, author);
            }
        });

        scheduleConcurrently(PARAMETERS.NUM_ITERATIONS_LOW, ignored_task_num -> {
            setRuntime();
            Map<String, Long> localm2A = instantiateMap(streamNameA);
            Map<String, Long> localm2B = instantiateMap(streamNameB);
            for (int i = 0; i < mapSize; i++) {
                assertThat(localm2A.get(String.valueOf(i))).isEqualTo((long) i);
                assertThat(localm2B.get(String.valueOf(i)) ).isEqualTo((long) 0);
            }
        });

        executeScheduled(PARAMETERS.CONCURRENCY_SOME, PARAMETERS.TIMEOUT_LONG);

        setRuntime();
        Map<String, Long> localm2A = instantiateMap(streamNameA);
        Map<String, Long> localm2B = instantiateMap(streamNameB);
        for (int i = 0; i < mapSize; i++) {
            assertThat(localm2A.get(String.valueOf(i)) ).isEqualTo((long)i);
            assertThat(localm2B.get(String.valueOf(i)) ).isEqualTo(0L);
        }

    }

    @Test
    @SuppressWarnings("checkstyle:magicnumber")
    public void periodicCkpointTrimTest() throws Exception {
        final String streamNameA = "mystreamA";
        final String streamNameB = "mystreamB";
        final String author = "periodicCkpoint";
        final int sizeAdjustment = 16; // size reduction to accomodate TRACE level debugging
        final int mapSize = PARAMETERS.NUM_ITERATIONS_MODERATE / sizeAdjustment;
        final int numThreads = PARAMETERS.CONCURRENCY_SOME + 2;

        long seed = System.currentTimeMillis();
        Random random = new Random(seed);
        int[] schedule = makeSchedule(numThreads, 200, random);
        log.debug("Iter ? A random seed {} numThreads {} sched {}\n", seed, numThreads, schedule);
        CoopScheduler.reset(numThreads);
        CoopScheduler.setSchedule(schedule);
        Thread ts[] = new Thread[numThreads+1];
        int tsIdx = 0;

        myRuntime = getDefaultRuntime().connect();

        Map<String, Long> m2A = instantiateMap(streamNameA);
        Map<String, Long> m2B = instantiateMap(streamNameB);


        ts[tsIdx++] = new Thread(() -> {
            int t = CoopScheduler.registerThread();
            sched();
            for (int i = 0; i < mapSize; i++) {
                log.debug("QQQ put iteration {}", i);
                sched();
                try { m2A.put(String.valueOf(i), (long)i); } catch (TrimmedException te) { log.error("PASS: DEBUG: whoo put m2A exception caught"); }
                sched();
                try { m2B.put(String.valueOf(i), 0L); }  catch (TrimmedException te) { log.error("PASS: DEBUG: whoo put m2B exception caught"); }
            }
            CoopScheduler.threadDone();
        });

        ts[tsIdx++] = new Thread(() -> {
            int t = CoopScheduler.registerThread();
            sched();
            CorfuRuntime currentRuntime = getMyRuntime();
            for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_VERY_LOW; i++) {
                // i'th checkpoint
                MultiCheckpointWriter mcw1 = new MultiCheckpointWriter();
                mcw1.addMap((SMRMap) m2A);
                mcw1.addMap((SMRMap) m2B);
                log.debug("QQQ checkpoint iteration {} before appendCheckpoints sizes {} and {}", i, m2A.size(), m2B.size());
                try {
                    sched();
                    long checkpointAddress = mcw1.appendCheckpoints(currentRuntime, author,
                            (x,y) -> { sched(); });
                    log.debug("QQQ checkpoint iteration {} mcw1 {}", i, mcw1.toString());

                    // Trim the log
                    sched();
                    currentRuntime.getAddressSpaceView().prefixTrim(checkpointAddress - 1);
                    log.debug("QQQ checkpoint iteration {} trim done", i);
                    sched();
                    currentRuntime.getAddressSpaceView().gc();
                    log.debug("QQQ checkpoint iteration {} gc done", i);
                    sched();
                    currentRuntime.getAddressSpaceView().invalidateServerCaches();
                    sched();
                    currentRuntime.getAddressSpaceView().invalidateClientCache();
                    log.debug("QQQ checkpoint iteration {} bottom", i);
                } catch (Exception e) {
                    System.err.printf("FAIL Whoa exception %s\n", e);
                    log.error("FAIL Whoa exception {}", e);
                }
            }
            CoopScheduler.threadDone();
        });

        for (int x = 0; x < PARAMETERS.CONCURRENCY_SOME; x++) {
            final int xx = x;
            ts[tsIdx++] = new Thread(() -> {
                int t = CoopScheduler.registerThread();
                sched();
                setRuntime();
                Map<String, Long> localm2A = instantiateMap(streamNameA);
                Map<String, Long> localm2B = instantiateMap(streamNameB);

                int currentMapSize = Integer.min(localm2A.size(), localm2B.size());
                for (int i = 0; i < currentMapSize; i++) {
                    log.debug("QQQ currentMapSize {}", currentMapSize);
                    Object gotval; // Use intermediate var for logging in error cases

                    sched();
                    gotval = localm2A.get(String.valueOf(i));
                    if (gotval == null) {
                        log.error("Null value at key {}, localm2A = {}", i, localm2A.toString());
                    }
                    log.trace("Check localm2A.get({}) -> {} by {} -> {}", i, gotval, Thread.currentThread().getName(), gotval != null && (Long) gotval == (long) i);
                    assertThat((Long) gotval).describedAs(Thread.currentThread().getName() + " A index " + i)
                            .isEqualTo((long) i);

                    sched();
                    gotval = localm2B.get(String.valueOf(i));
                    if (gotval == null) {
                        log.error("Null value at key {}, localm2B = {}", i, localm2B.toString());
                    }
                    log.trace("Check localm2B.get({}) -> {} by {} -> {}", i, gotval, Thread.currentThread().getName(), gotval != null && (Long) gotval == (long) 0);
                    assertThat((Long) gotval).describedAs(Thread.currentThread().getName() + " B index " + i)
                            .isEqualTo((long) 0);
                }
                CoopScheduler.threadDone();
            });
        }

        for (int i = 0; i < ts.length; i++) { if (ts[i] != null) ts[i].start(); }
        CoopScheduler.runScheduler(tsIdx);
        for (int i = 0; i < ts.length; i++) { try { if (ts[i] != null) ts[i].join(); } catch (Exception e) { log.error("FAIL join exception {}", e); } }

        setRuntime();
        log.trace("INSTANTIATE final maps");
        Map<String, Long> localm2A = instantiateMap(streamNameA);
        Map<String, Long> localm2B = instantiateMap(streamNameB);
        for (int i = 0; i < mapSize; i++) {
            if (localm2A.get(String.valueOf(i)) == null || localm2A.get(String.valueOf(i)) != i) { log.warn("BUMMER i = {} localm2A = {}", i, localm2A); }
            assertThat(localm2A.get(String.valueOf(i)) ).isEqualTo((long)i);
            if (localm2A.size() != mapSize) { log.warn("BUMMER mapSize {} localm2A = {}", mapSize, localm2A); }
            assertThat(localm2A).hasSize(mapSize);
            if (localm2B.get(String.valueOf(i)) == null || localm2B.get(String.valueOf(i)) != 0) { log.warn("BUMMER i = {} localm2B = {}", i, localm2B); }
            assertThat(localm2B.get(String.valueOf(i)) ).isEqualTo(0L);
            if (localm2B.size() != mapSize) { log.warn("BUMMER mapSize {} localm2B = {}", mapSize, localm2B); }
            assertThat(localm2B).hasSize(mapSize);
        }
    }

    @Test @SuppressWarnings("checkstyle:magicnumber")
    public void YooTest() throws Exception {
        int lim = 55;
        for (int i = 0; i < lim; i++) {
            periodicCkpointTrimTest();
        }
    }
    @SuppressWarnings("checkstyle:magicnumber")
    public int[] makeSchedule(int maxThreads, int length, Random random) {
        int[] schedule = new int[length];
        final int winnerThisTime = 20;
        boolean winnerPossible = random.nextInt(100) < winnerThisTime;
        final int winnerProb = 10;
        final int winnerMaxLen = 40;

        for (int i = 0; i < schedule.length; i++) {
            // Sometimes create an unfair schedule for a lucky winner thread.
            if (winnerPossible && random.nextInt(winnerProb) == 0) {
                int winner = random.nextInt(maxThreads);
                int repeats = random.nextInt(winnerMaxLen);
                while (i < schedule.length && repeats-- > 0) {
                    schedule[i++] = winner;
                }
            } else {
                schedule[i] = random.nextInt(maxThreads);
            }
        }
        return schedule;
    }
}
