package org.corfudb.runtime.concurrent;

import com.brein.time.timeintervals.collections.SetIntervalCollection;
import com.brein.time.timeintervals.indexes.IntervalTree;
import com.brein.time.timeintervals.indexes.IntervalTreeBuilder;
import com.brein.time.timeintervals.intervals.IInterval;
import com.brein.time.timeintervals.intervals.LongInterval;
import org.corfudb.runtime.collections.SMRMap;
import org.corfudb.runtime.object.transactions.AbstractTransactionsTest;
import org.corfudb.util.LogIntervalReplicas;
import org.corfudb.util.RepairScanner;
import org.corfudb.util.ScannerWorkStatus;
import org.junit.Test;

import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;
import java.util.zip.CheckedOutputStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 *
 */

public class RepairScanTest extends AbstractTransactionsTest {

    @Override
    public void TXBegin() {
        OptimisticTXBegin();
    }

    @Test
    @SuppressWarnings("checkstyle:magicnumber")
    public void fooTest() throws Exception {
        final IntervalTree tree = IntervalTreeBuilder.newBuilder()
                .usePredefinedType(IntervalTreeBuilder.IntervalType.LONG)
                // WHOA ... our .overlap() query will find nothing if we omit a .collectIntervals()   {sigh}
                // .collectIntervals(interval -> new ListIntervalCollection())
                .collectIntervals(interval -> new SetIntervalCollection())
                .build();
        tree.add(new LongInterval(1L, 5L));
        tree.add(new LongInterval(2L, 5L));
        tree.add(new LongInterval(3L, 5L));

        final Collection<IInterval> overlap = tree.overlap(new LongInterval(2L, 2L));
        overlap.forEach(System.out::println); // will print out [1, 5] and [2, 5]

        final Collection<IInterval> find = tree.find(new LongInterval(2L, 5L));
        find.forEach(System.out::println);    // will print out only [2, 5]
    }

    @Test
    @SuppressWarnings("checkstyle:magicnumber")
    public void barTest() throws Exception {
        String mapName1 = "testMapBar";
        Map<Object, Object> testMap1 = instantiateCorfuObject(SMRMap.class, mapName1);

        testMap1.put("foo", 42);
        testMap1.put(42.0, new String[]{"bar"});

        System.err.printf("map %s -> %s\n", "foo", testMap1.get("foo"));
        System.err.printf("map %s -> %s\n", 42, testMap1.get(42));
        System.err.printf("map %s -> %s\n", 42.0, testMap1.get(42.0));
    }

    @Test
    @SuppressWarnings("checkstyle:magicnumber")
    public void healthyMapTest() throws Exception {
        RepairScanner rs = new RepairScanner(getDefaultRuntime());
        Set<String> setA = Collections.singleton("host:0");
        IInterval<Long> bigInterval = new LongInterval(0L, 99L);
        IInterval<Long> smallInterval = new LongInterval(0L, 9L);
        LogIntervalReplicas lirA = new LogIntervalReplicas(
                Collections.singleton(smallInterval), setA);

        assertThat(rs.getGlobalUnknownMap()).isEmpty();

        assertThat(rs.replaceGlobalUnknownMap(bigInterval, lirA)).isFalse();
        assertThat(rs.addToGlobalUnknownMap(bigInterval, lirA)).isTrue();
        assertThat(rs.addToGlobalUnknownMap(bigInterval, lirA)).isFalse();
        assertThat(rs.replaceGlobalUnknownMap(bigInterval, lirA)).isTrue();

        assertThat(rs.deleteFromGlobalUnknownMap(bigInterval)).isTrue();
        assertThat(rs.deleteFromGlobalUnknownMap(bigInterval)).isFalse();
    }

    @Test
    @SuppressWarnings("checkstyle:magicnumber")
    public void unknownMapTest() throws Exception {
        RepairScanner rs = new RepairScanner(getDefaultRuntime());
        IInterval<Long> intervalA = new LongInterval(0L, 99L);
        Set<String> setA = Collections.singleton("host:0");
        Set<String> setB = Collections.singleton("host:1");

        assertThat(rs.getGlobalHealthyMap()).isEmpty();
        assertThat(rs.addToGlobalHealthyMap(intervalA, setA)).isTrue();
        assertThat(rs.addToGlobalHealthyMap(intervalA, setB)).isFalse();
        assertThat(rs.deleteFromGlobalHealthyMap(intervalA)).isTrue();
        assertThat(rs.deleteFromGlobalHealthyMap(intervalA)).isFalse();
    }

    @Test
    @SuppressWarnings("checkstyle:magicnumber")
    public void workingMapTest() throws Exception {
        RepairScanner rs = new RepairScanner(getDefaultRuntime());
        String myName = rs.myWorkerName();
        ScannerWorkStatus status = new ScannerWorkStatus(
                LocalDateTime.now(),
                LocalDateTime.now(),
                new LongInterval(1L, 5L),
                0);

        assertThat(rs.getGlobalWorkingMap()).isEmpty();
        assertThat(rs.addToGlobalWorkingMap(myName, status)).isTrue();
        assertThat(rs.addToGlobalWorkingMap(myName, status)).isFalse();
        assertThat(rs.deleteFromGlobalWorkingMap(myName)).isTrue();
        assertThat(rs.deleteFromGlobalWorkingMap(myName)).isFalse();
    }

    @Test
    public void sketchWorkflowTest() throws Exception {
        Random random = new Random();
        long seed = System.currentTimeMillis();
        random.setSeed(seed);
        RepairScanner rs = new RepairScanner(getDefaultRuntime());
        IInterval<Long> globalInterval = new LongInterval(0L, 19L);
        Set<String> setA = new HashSet<String>();
        setA.add("hostA:9000");
        setA.add("hostB:9000");
        setA.add("hostC:9000");
        LogIntervalReplicas unknownState = new LogIntervalReplicas(
                Collections.singleton(globalInterval), setA);

        // Pretend that we look at a layout and see 0-19 using hostA/B/C.
        // We've never seen this interval before, so we add this
        // interval to the unknown map.
        rs.addToGlobalUnknownMap(globalInterval, unknownState);

        // Pretend that there are some active workers that
        // have already been started.
        Set<IInterval<Long>> otherActiveWorkers = new HashSet<>();
        otherActiveWorkers.add(new LongInterval(1L, 2L));
        otherActiveWorkers.add(new LongInterval(4L, 4L));
        otherActiveWorkers.add(new LongInterval(11L, 12L));
        otherActiveWorkers.forEach(i ->
                rs.addToGlobalWorkingMap("worker" + i.getNormStart().toString(),
                        new ScannerWorkStatus(LocalDateTime.now(), LocalDateTime.now(), i, 0)));

        final int numThreads = 11;
        // Pretend that we have 11 threads that want to do work.
        // Iterations 9 and beyond will have no work available,
        // but we'll just test the last iteration.
        List<IInterval<Long>> ourIntervals = new ArrayList<>();
        for (int i = 0; i < numThreads; i++) {
            // Story: Skip past parts of chosenWorkInterval that have
            // active workers to select a subinterval to work on.
            IInterval<Long> found = rs.findIdleInterval();
            // System.err.printf("*** iter %d found = %s\n", i, found);
            if (found != null) {
                rs.addToGlobalWorkingMap("foo" + found.getNormStart().toString(),
                        new ScannerWorkStatus(LocalDateTime.now(), LocalDateTime.now(), found, 0));
                ourIntervals.add(found);
            }
            if (i == (numThreads-1)) {
                assertThat(found).isNull();
            }
        }

        // Sanity check: build an interval tree with the ourIntervals (A) intervals
        // and with the otherActiveWorkers (B) intervals.  For each log address
        // in globalInterval, that address must overlap with exactly 1 of {A,B}.
        IntervalTree treeA = IntervalTreeBuilder.newBuilder()
                .usePredefinedType(IntervalTreeBuilder.IntervalType.LONG)
                .collectIntervals(interval -> new SetIntervalCollection())
                .build();
        IntervalTree treeB = IntervalTreeBuilder.newBuilder()
                .usePredefinedType(IntervalTreeBuilder.IntervalType.LONG)
                .collectIntervals(interval -> new SetIntervalCollection())
                .build();
        ourIntervals.stream().forEach(i -> treeA.add(i));
        otherActiveWorkers.stream().forEach(i -> treeB.add(i));
        for (Long addr = globalInterval.getNormStart(); addr <= globalInterval.getNormEnd(); addr++) {
            IInterval<Long> addrI = new LongInterval(addr, addr);
            int sum = treeA.overlap(addrI).size() + treeB.overlap(addrI).size();
            assertThat(sum).isEqualTo(1);
        }

        // Simulate completion of the work, updating the
        // global state as appropriate.  We shuffle-sort the
        // ourIntervals to help find bugs.
        try {
            ourIntervals.stream()
                    .sorted((a, b) -> random.nextInt(100) < 50 ? -1 : 1)
                    .forEach(i -> {
                        String workerKey = (String) rs.getGlobalWorkingMap().entrySet().stream()
                                .filter(e -> e.getValue().getInterval().equals(i))
                                .map(e -> e.getKey())
                                .toArray()[0];
                        rs.workerSuccess(workerKey, i);
                    });
        } catch (Exception e) {
            System.err.printf("Error with seed = %d\n", seed);
            throw e;
        }

        // Now that all of ourIntervals have simulated doing
        // successful work and should have cleaned up all
        // their state, anything else that remains in the
        // global working list must be the equivalent to the
        // otherActiveWorkers set.
        assertThat(rs.getGlobalWorkingMap().values().stream()
        .map(x -> x.getInterval()).collect(Collectors.toSet())).isEqualTo(otherActiveWorkers);
        // System.err.printf("FINAL unknown map keys: %s\n", rs.getGlobalUnknownMap().keySet());
    }

}