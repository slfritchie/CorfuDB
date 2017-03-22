package org.corfudb.util;

import org.apache.commons.lang.math.RandomUtils;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class CoopScheduler {

    static int maxThreads = -1;
    static int numThreads;
    static public ThreadLocal<Integer> threadMap;
    static HashSet<Integer> thrSet;
    static boolean centralStopped;
    static final Object centralReady = new Object();
    static boolean ready[];
    static boolean done[];
    static Integer go[];
    static int schedule[];

    static List log;

    public static void reset(int maxT) {
        maxThreads = maxT;
        numThreads = 0;
        threadMap = ThreadLocal.withInitial(() -> -1);
        thrSet = new HashSet<>();
        centralStopped = false;
        ready = new boolean[maxThreads];
        done = new boolean[maxThreads];
        go = new Integer[maxThreads];
        for (int i = 0; i < maxThreads; i++) {
            ready[i] = false;
            done[i] = false;
            go[i] = 0;
        }
        schedule = null;
        log = Collections.synchronizedList(new ArrayList<String>());
    }

    public static int registerThread() {
        return registerThread(-1);
    }

    public static int registerThread(int t) {
        synchronized (centralReady) {
            if (centralStopped == true) {
                System.err.printf("Central scheduler has stopped scheduling, TODO\n");
                return -1;
            }
            if (threadMap.get() >= 0) {
                System.err.printf("Thread already registered, TODO\n");
                return -1;
            }
            if (t < 0) {
                t = numThreads++;
            } else {
                // Caller asked for a specific number.  Let's avoid duplicates.
                if (thrSet.contains(t)) {
                    System.err.printf("Thread id %d already in use, TODO\n", t);
                    return -1;
                }
                if (t >= numThreads) {
                    numThreads = t + 1;
                }
            }
            if (t >= maxThreads) {
                System.err.printf("Too many threads, TODO\n");
                return -1;
            }
            threadMap.set(t);
            thrSet.add(t);
            // Don't set ready[t] here.  Instead, do it near top of sched().
            return t;
        }
    }

    public static void threadDone() {
        threadDone(threadMap.get());
    }

    public static void threadDone(int thread) {
        try {
            done[thread] = true;
            synchronized (centralReady) {
                centralReady.notifyAll();
            }
        } catch (Exception e) {
            System.err.printf("ERROR: threadDone() exception %s\n", e.toString());
            return;
        }
    }

    public static void setSchedule(int schedIn[]) {
        HashSet<Integer> present = new HashSet<>();
        ArrayList<Integer> missing = new ArrayList<>();
        int numMissing = 0;

        for (int i = 0; i < schedIn.length; i++) {
            present.add(schedIn[i]);
        }
        for (int i = 0; i < maxThreads; i++) {
            if (! present.contains(i)) {
                missing.add(i);
                numMissing++;
            }
        }
        missing.sort(Comparator.naturalOrder());

        int sched[] = new int[schedIn.length + numMissing];
        for (int i = 0; i < schedIn.length; i++) {
            sched[i] = schedIn[i];
        }
        for (int i = 0; i < numMissing; i++) {
            sched[schedIn.length + i] = missing.get(i);
        }

        schedule = sched;

        /****** System.err.printf("\nschedule = {");
        for (int i = 0; i < schedule.length; i++) {
            System.err.printf("%d,", schedule[i]);
        }
        System.err.printf("}\n"); ******/
    }

    private static int schedErrors = 0;
    public static void sched() {
        try {
            sched(threadMap.get());
        } catch (Exception e) {
            if (schedErrors++ < 3) {
                System.err.printf("ERROR: sched() exception %s\n", e.toString());
            } else if (schedErrors < 5) {
                System.err.printf("sched() exception warnings suppressed\n");
            }
            return;
        }
    }

    public static void sched(int t) {
        // if (t >= 0) { System.err.printf("s%d,\n", t); }
        if (done[t]) {
            System.err.printf("ERROR: thread has called threadDone()!\n");
            return;
        }
        try {
            synchronized (centralReady) {
                ready[t] = true;
                go[t] = 0;
                centralReady.notifyAll();
            }
            synchronized (centralReady) {
                while (!centralStopped && go[t] == 0) {
                    centralReady.wait();
                }
                if (centralStopped) {
                    System.err.printf("NOTICE scheduler stopped\n"); return;
                }
                if (go[t] != 1) { System.err.printf("TODO invariant violation\n"); }
            }
        } catch (InterruptedException e) {
            System.err.printf("sched nterrupted, TODO?\n");
            sched(t);
        }
    }

    public static void runScheduler(int numStartingThreads) {
        int t;
        int given = 0;

        while (! someReady(numStartingThreads)) {
            try { Thread.sleep(1); } catch (Exception e) {}
        }
        // System.err.printf("SCHED: entering main loop\n");
        while (true) {
            if (allDone()) {
                synchronized (centralReady) {
                    if (allDone()) {
                        centralStopped = true;
                        break;
                    }
                }
            }
            for (int i = 0; i < schedule.length; i++) {
                t = schedule[i];
                if (! ready[t] || done[t]) { continue; }
                synchronized (centralReady) {
                    while (go[t] != 0) {
                        try {centralReady.wait();} catch (InterruptedException e) { System.err.printf("TODO BUMMER FIX ME\n"); return; }
                    }
                    go[t] = 1;
                    centralReady.notifyAll();
                    given++;
                }

                synchronized (centralReady) {
                    while (!done[t] && go[t] != 0) {
                        try {centralReady.wait();} catch (InterruptedException e) { System.err.printf("TODO BUMMER FIX ME\n"); return; }
                    }
                }
            }
        }
        // System.err.printf("GIVEN = %d,", given);
    }

    private static boolean someReady(int maxThr) {
        for (int i = 0; i < maxThr; i++) {
            if (! ready[i]) {
                return false;
            }
        }
        return true;
    }

    private static boolean allDone() {
        for (int i = 0; i < numThreads; i++) {
            if (ready[i] && ! done[i]) {
                return false;
            }
        }
        return true;
    }

    public static void appendLog(String s) {
        synchronized (log) {
            log.add(s);
        }
    }

    public static String[] getLog() {
        synchronized (log) {
            String l[] = new String[log.size()];
            int src = log.size() - 1;
            for (int dst = 0; dst < log.size(); src--, dst++) {
                l[dst] = (String) log.get(src);
            }
            return l;
        }
    }

    public static boolean logsAreIdentical(ArrayList<String[]> logs) {
        for (int i = 0; i < logs.size() - 2; i++) {
            String[] l1 = logs.get(i);
            String[] l2 = logs.get(i+1);
            if (l1.length != l2.length) { return false; }
            for (int j = 0; j < l1.length; j++) {
                if (! l1[j].equals(l2[j])) { return false; }
            }
        }
        return true;
    }

    public static int[] makeSchedule(int maxThreads, int length) {
        int[] schedule = new int[length];
        final int winnerThisTime = 10;
        boolean winnerPossible = RandomUtils.nextInt(100) < 10;
        final int winnerProb = 10;
        final int winnerMaxLen = 40;

        for (int i = 0; i < schedule.length; i++) {
            // Sometimes create an unfair schedule for a lucky winner thread.
            if (winnerPossible && RandomUtils.nextInt(winnerProb) == 0) {
                int winner = RandomUtils.nextInt(maxThreads);
                int repeats = RandomUtils.nextInt(winnerMaxLen);
                while (i < schedule.length && repeats-- > 0) {
                    schedule[i++] = winner;
                }
            } else {
                schedule[i] = RandomUtils.nextInt(maxThreads);
            }
        }
        return schedule;
    }

    public static String formatSchedule(int[] schedule) {
        String s = new String("{");
        for (int i = 0; i < schedule.length; i++) {
            s += ((i == 0) ? "" : ",") + schedule[i];
        }
        s += "}";
        return s;
    }

    public static void printSchedule(int[] schedule) {
        System.err.printf("schedule = %s\n", formatSchedule(schedule));
    }


}
