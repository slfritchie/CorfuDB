package org.corfudb.util;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class CoopScheduler {

    static int maxThreads = -1;
    static AtomicInteger numThreads;
    static public ThreadLocal<Integer> threadMap;
    static boolean centralStopped;
    static final Object centralReady = new Object();
    static boolean ready[];
    static boolean done[];
    static Integer go[];
    static int schedule[];

    static List log;

    public static void reset(int maxT) {
        maxThreads = maxT;
        numThreads = new AtomicInteger(0);
        threadMap = ThreadLocal.withInitial(() -> -1);
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
        synchronized (centralReady) {
            if (centralStopped == true) {
                System.err.printf("Central scheduler has stopped scheduling, TODO");
                return -1;
            }
            if (threadMap.get() >= 0) {
                System.err.printf("Thread already registered, TODO");
                return -1;
            }
            int t = numThreads.getAndIncrement();
            if (t >= maxThreads) {
                System.err.printf("Too many threads, TODO");
                return -1;
            }
            threadMap.set(t);
            // Don't set ready[t] here.  Instead, do it near top of sched().
            return t;
        }
    }

    public static void threadDone() {
        threadDone(threadMap.get());
    }

    public static void threadDone(int thread) {
        done[thread] = true;
        synchronized (centralReady) {
            centralReady.notifyAll();
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

    public static void sched() {
        try {
            sched(threadMap.get());
        } catch (Exception e) {
            System.err.printf("ERROR: sched() exception %s\n", e.toString());
            return;
        }
    }

    public static void sched(int t) {
        System.err.printf("s%d,\n", t);
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

        while (! someReady(numStartingThreads)) {
            try { Thread.sleep(1); } catch (Exception e) {}
        }
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
                }

                synchronized (centralReady) {
                    while (!done[t] && go[t] != 0) {
                        try {centralReady.wait();} catch (InterruptedException e) { System.err.printf("TODO BUMMER FIX ME\n"); return; }
                    }
                }
            }
        }
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
        int numThr = numThreads.get();
        for (int i = 0; i < numThr; i++) {
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
}
