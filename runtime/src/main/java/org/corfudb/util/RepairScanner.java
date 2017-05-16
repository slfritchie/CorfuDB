package org.corfudb.util;

import com.brein.time.timeintervals.collections.SetIntervalCollection;
import com.brein.time.timeintervals.indexes.IntervalTree;
import com.brein.time.timeintervals.indexes.IntervalTreeBuilder;
import com.brein.time.timeintervals.intervals.IInterval;
import com.brein.time.timeintervals.intervals.LongInterval;
import lombok.Getter;
import lombok.Setter;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.SMRMap;
import org.corfudb.runtime.view.Layout;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.UnknownHostException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Corfu global log scanner & replica repair.
 */

public class RepairScanner {
    private CorfuRuntime rt;
    private final String globalStateStreamName = "Repair scanner global state";
    private Map<Object, Object> globalState;
    private final Long initTime = System.nanoTime();
    @Getter
    private Random random = new Random();
    @Getter
    @Setter
    private long workerTimeoutSeconds = 1; // TODO: put me back: (5*60);
    @Getter
    @Setter
    private long batchSize = 2; // TODO: put me back: 5000

    // Map<IInterval<Long>, Set<String>>
    private final String keyHealthyMap = "Healthy map";
    // Map<IInterval<Long>, LogIntervalReplicas>
    private final String keyUnknownMap = "Unknown map";
    // Map<String, ScannerWorkStatus>
    private final String keyWorkingMap = "Working map";
    // Map<Long, CorfuLayout>
    private final String keyLayoutMap = "Layout archive map";

    public RepairScanner(CorfuRuntime rt) {
        this.rt = rt;
        globalState = rt.getObjectsView().build()
                .setStreamName(globalStateStreamName)
                .setType(SMRMap.class)
                .open();

        try {
            rt.getObjectsView().TXBegin();
            if (getHealthyMap() == null) {
                globalState.put(keyHealthyMap, new HashMap<IInterval<Long>, Set<String>>());
            }
            if (getUnknownMap() == null) {
                globalState.put(keyUnknownMap, new HashMap<IInterval<Long>, LogIntervalReplicas>());
            }
            if (getWorkingMap() == null) {
                globalState.put(keyWorkingMap, new HashMap<IInterval<Long>, ScannerWorkStatus>());
            }
            if (getLayoutMap() == null) {
                globalState.put(keyWorkingMap, new HashMap<Long, Layout>());
            }

            rt.getObjectsView().TXEnd();
        } catch (Exception e) {
            System.err.printf("TODO: error: %s\n", e);
        }

    }

    public Set<Set<String>> getUnknownState_uniqueReplicas() {
        // Map<IInterval<Long>, LogIntervalReplicas>
        Set<Set<String>> x = getUnknownMap().values().stream()
                .map(lir -> lir.getReplicaSet())
                .collect(Collectors.toSet());
        return x;
    }

    public Set<IInterval<Long>> getUnknownState_globalRanges(Set<String> replicaSet) {
        // Map<IInterval<Long>, LogIntervalReplicas>
        Set<IInterval<Long>> x = getUnknownMap().entrySet().stream()
                .filter(e -> e.getValue().getReplicaSet().equals(replicaSet))
                .map(e -> e.getKey())
                .collect(Collectors.toSet());
        return x;
    }

    /**
     * Find an idle interval to work on.  We direct our search by
     * first looking at unknown state replica sets, then at an
     * overall log interval for a replica set, then a specific
     * log interval that isn't yet being worked on.
     *
     * @return Interval to work on, or null
     * if there is no work available.
     */

    public IInterval<Long> findIdleInterval() {
        // Story: For load balancing reasons, we first want to
        // chose a replica set at random.
        Object x[], y[], z[];
        int index;
        Function<Object,Integer> compare = (_dontCare) -> (random.nextInt(100) < 50) ? -1 : 1;

        x = getUnknownState_uniqueReplicas().stream()
                .sorted((a,b) -> compare.apply(a)).toArray();
        for (int ix = 0; ix < x.length; ix++) {
            Set<String> chosenReplicaSet = (Set<String>) x[ix];
            // System.err.printf("Candidate replica set = %s\n", chosenReplicaSet);
            y = getUnknownState_globalRanges(chosenReplicaSet).stream().
                    sorted((a,b) -> compare.apply(a)).toArray();
            for (int iy = 0; iy < y.length; iy++) {
                IInterval<Long> chosenGlobalInterval = (IInterval<Long>) y[iy];
                // System.err.printf("Candidate global interval = %s\n", chosenGlobalInterval);

                // Find & delete any working entries that are too old.
                LocalDateTime tooLate = LocalDateTime.now()
                        .minus(Duration.ofSeconds(getWorkerTimeoutSeconds()));
                getWorkingMap().entrySet().stream()
                        .filter(e -> e.getValue().getUpdateTime().compareTo(tooLate) < 0)
                        .forEach(e -> {
                            String workerKey = e.getKey();
                            System.err.printf("***** worker key %s has expired (worker %s tooLate %s), deleting\n", workerKey, e.getValue().getUpdateTime(), tooLate);
                            deleteFromGlobalWorkingMap(workerKey);
                        });

                z = getUnknownMap().get(chosenGlobalInterval).getLogIntervalSet().toArray();
                for (int iz = 0; iz < z.length; iz++) {
                    IInterval<Long> workInterval = (IInterval<Long>) z[iz];
                    // System.err.printf("Candidate work interval = %s\n", workInterval);
                    IInterval<Long> res = findIdleInterval2(workInterval);
                    if (res != null) {
                        return res;
                    }
                }
            }
        }
        return null;
    }
    
    public IInterval<Long> findIdleInterval2(IInterval<Long> candidateWorkInterval) {
        Long iStart = candidateWorkInterval.getNormStart();
        Long iEnd = candidateWorkInterval.getNormEnd();
        Long incr = getBatchSize() - 1;
        IInterval<Long> found = null;

        // Build interval tree to help identify intervals that
        // already have active workers.
        IntervalTree tree = IntervalTreeBuilder.newBuilder()
                .usePredefinedType(IntervalTreeBuilder.IntervalType.LONG)
                .collectIntervals(interval -> new SetIntervalCollection())
                .build();
        getWorkingMap().entrySet().stream()
                .map(e -> e.getValue().getInterval())
                .forEach(i -> tree.add(i));

        // Search the interval
        while (iStart <= iEnd) {
            // System.err.printf("top: overlap search for %s\n", new LongInterval(iStart, iStart + incr));
            Collection<IInterval> c = tree.overlap(new LongInterval(iStart, iStart + incr));
            if (c.isEmpty()) {
                found = new LongInterval(iStart, Long.min(iStart + incr, iEnd));
                // System.err.printf("found %s, break\n", found);
                break;
            } else {
                IInterval<Long> conflict = (IInterval<Long>) c.toArray()[0];
                // System.err.printf("CONFLICT %s\n", conflict);
                if (conflict.getNormStart() == iStart) {
                    iStart = conflict.getNormEnd() + 1;
                    incr = getBatchSize() - 1;
                    // System.err.printf("bottom: A: iStart %d incr %d\n", iStart, incr);
                } else {
                    // Keep iStart the same, but use a smaller incr and retry.
                    // There may be *another* overlap, hence the retry.
                    incr = conflict.getNormStart() - iStart - 1;
                    // System.err.printf("bottom: B: iStart %d incr %d\n", iStart, incr);
                }
            }
        }
        return found;
    }

    public boolean workerAborted(String workerID, IInterval<Long> workInterval) {
        if (getWorkingMap().get(workerID).equals(workerID)) {
            return deleteFromGlobalWorkingMap(workerID);
        } else {
            return false;
        }
    }

    public boolean workerSuccess(String workerID, IInterval<Long> workInterval) {
        if (getWorkingMap().get(workerID) != null) {
            final IntervalTree tree = IntervalTreeBuilder.newBuilder()
                    .usePredefinedType(IntervalTreeBuilder.IntervalType.LONG)
                    .collectIntervals(interval -> new SetIntervalCollection())
                    .build();
            getUnknownMap().keySet().stream()
                    .forEach(interval -> tree.add(interval));
            IInterval<Long> unknownMapKey = (IInterval<Long>) tree.overlap(workInterval).toArray()[0];

            LogIntervalReplicas moveVal = getUnknownMap().get(unknownMapKey);
            if (workInterval.equals(unknownMapKey)) {
                // 100% overlap.  Nothing to add.
                // Let the deleteFromGlobalWorkingMap() do the rest.
            } else if (workInterval.getNormStart() == unknownMapKey.getNormStart()) {
                IInterval<Long> newKey = new LongInterval(workInterval.getNormEnd() + 1, unknownMapKey.getNormEnd());
                addToGlobalUnknownMap(newKey, moveVal);
            } else if (workInterval.getNormEnd() == unknownMapKey.getNormEnd()) {
                IInterval<Long> newKey = new LongInterval(unknownMapKey.getNormStart(), workInterval.getNormStart() - 1);
                addToGlobalUnknownMap(newKey, moveVal);
            } else {
                IInterval<Long> beforeKey = new LongInterval(unknownMapKey.getNormStart(), workInterval.getNormStart() - 1);
                IInterval<Long> afterKey = new LongInterval(workInterval.getNormEnd() + 1, unknownMapKey.getNormEnd());
                addToGlobalUnknownMap(beforeKey, moveVal);
                addToGlobalUnknownMap(afterKey, moveVal);
            }
            deleteFromGlobalUnknownMap(unknownMapKey);
            boolean yoo =
            deleteFromGlobalWorkingMap(workerID);
            return true;
        } else {
            return false;
        }
    }


        /**************************************/

    // @VisibleForTesting private
    public
    Map<IInterval<Long>, Set<String>> getHealthyMap() {
        return (Map<IInterval<Long>, Set<String>>) globalState.get(keyHealthyMap);
    }

    // @VisibleForTesting private
    public
    Map<IInterval<Long>, LogIntervalReplicas> getUnknownMap() {
        return (Map<IInterval<Long>, LogIntervalReplicas>) globalState.get(keyUnknownMap);
    }

    // @VisibleForTesting private
    public
    Map<String, ScannerWorkStatus> getWorkingMap() {
        return (Map<String, ScannerWorkStatus>) globalState.get(keyWorkingMap);
    }

    // @VisibleForTesting private
    public
    Map<Long, Layout> getLayoutMap() {
        return (Map<Long, Layout>) globalState.get(keyLayoutMap);
    }

    // @VisibleForTesting private
    public
    boolean deleteFromGlobalHealthyMap(IInterval<Long> interval) {
        Map<IInterval<Long>, Set<String>> m = getHealthyMap();
        if (m.containsKey(interval)) {
            m.remove(interval);
            return true;
        } else {
            return false;
        }
    }

    // @VisibleForTesting private
    public
    boolean addToGlobalHealthyMap(IInterval<Long> interval, Set<String> replicas) {
        Map<IInterval<Long>, Set<String>> m = getHealthyMap();
        // TODO more safety/sanity checking here, e.g., overlapping conditions

        if (! m.containsKey(interval)) {
            getHealthyMap().put(interval, replicas);
            return true;
        } else {
            return false;
        }
    }

    // @VisibleForTesting private
    public
    boolean deleteFromGlobalUnknownMap(IInterval<Long> interval) {
        Map<IInterval<Long>, LogIntervalReplicas> m = getUnknownMap();
        if (m.containsKey(interval)) {
            m.remove(interval);
            return true;
        } else {
            return false;
        }
    }

    // @VisibleForTesting private
    public
    boolean addToGlobalUnknownMap(IInterval<Long> interval, LogIntervalReplicas lir) {
        Map<IInterval<Long>, LogIntervalReplicas> m = getUnknownMap();
        // TODO more safety/sanity checking here, e.g., overlapping conditions

        if (! m.containsKey(interval)) {
            m.put(interval, lir);
            return true;
        } else {
            return false;
        }
    }

    // @VisibleForTesting private
    public
    boolean replaceGlobalUnknownMap(IInterval<Long> interval, LogIntervalReplicas lir) {
        Map<IInterval<Long>, LogIntervalReplicas> m = getUnknownMap();
        // TODO more safety/sanity checking here, e.g., overlapping conditions

        if (! m.containsKey(interval)) {
            return false;
        } else {
            m.put(interval, lir);
            return true;
        }
    }

    // @VisibleForTesting private
    public
    boolean deleteFromGlobalWorkingMap(String workerName) {
        Map<String, ScannerWorkStatus> m = getWorkingMap();
        if (m.containsKey(workerName)) {
            m.remove(workerName);
            return true;
        } else {
            return false;
        }
    }

    // @VisibleForTesting private
    public
    boolean addToGlobalWorkingMap(String workerName, ScannerWorkStatus status) {
        Map<String, ScannerWorkStatus> m = getWorkingMap();
        // TODO more safety/sanity checking here, e.g., overlapping conditions

        if (! m.containsKey(workerName)) {
            m.put(workerName, status);
            return true;
        } else {
            return false;
        }
    }

    /**************************************/

    public String myWorkerName() {
        try {
            return myWorkerName(java.net.InetAddress.getLocalHost().toString());
        } catch (UnknownHostException e) {
            return myWorkerName("unknown host");
        }
    }

    public String myWorkerName(String host) {
        String whoAmI = getWhoAmI();
        String thr = Thread.currentThread().toString();
        return String.join(",", host, whoAmI, thr,
                initTime.toString());
    }

    public String getWhoAmI() {
        BufferedReader reader;
        try {
            // TODO external program review?
            java.lang.Process p = Runtime.getRuntime().exec("/usr/bin/who am i");
            p.waitFor();

            reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
            String line = reader.readLine();
            String a[] = line.split(" +");
            return String.join(":", a[0], a[1]);
        } catch (Exception e) {
            return getWhoAmI();
        }


    }
}
