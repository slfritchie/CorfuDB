package org.corfudb.util;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.math.RandomUtils;

import java.util.*;

/** A cooperative scheduler framework for deterministic thread
 *  scheduling (or as deterministic-as-feasible).
 *
 *  A typical workflow for threads that wish to use this
 *  scheduler will go through these stages:
 *
 *  1. A coordinating thread calls reset(Max) to clear the central
 *     scheduler's state, where Max is the limit # of threads that
 *     can the central scheduler may manage.
 *
 *  2. One thread (typically the coordinator) sets the execution
 *     schedule with the central scheduler via setSchedule().  A
 *     schedule is an array integers or list of CoopQuantum where
 *     the next thread to be scheduled is chosen round-robin from
 *     the schedule array/list.  An item in the schedule is
 *     ignored if the thread ID chosen has not yet registered,
 *     if that thread has exited the scheduler via threadDone(),
 *     or if the thread is blocking on an operation that cannot
 *     be managed by this scheduler (as indicated by calling
 *     the withdraw() function).
 *
 *     See makeSchedule() for one method for generating a
 *     schedule.
 *
 *  3. All participating threads indicate their participation
 *     with a call to registerThread().
 *     Each thread is assigned a unique thread ID integer, up
 *     to the limit set by the reset(Max) call.
 *     If the new thread ID does not appear in the schedule,
 *     the ID will be added.  However, only a single entry
 *     will be added: if the schedule is a long list, then the
 *     result will be infrequent scheduling.
 *
 *  4. Whenever a thread wishes to permit the central scheduler
 *     to make a scheduling change, it calls sched().
 *
 *  5. All participating threads must eventually call
 *     threadDone().
 *
 *  6. Meanwhile, one thread (typically the coordinator) starts
 *     scheduling assignments by calling runScheduler().  This
 *     function returns when all threads have called
 *     threadDone().
 */

@Slf4j
public class CoopScheduler {
    private static class CoopThreadStatus {
        boolean ready = false;
        boolean done = false;
        Integer ticks = 0;
    };

    public static class CoopQuantum {
        int thread;
        int ticks;

        public CoopQuantum(int thread, int ticks) {
            this.thread = thread;
            this.ticks = ticks;
        }
    };

    static int maxThreads = -1;
    static int numThreads;
    static public ThreadLocal<Integer> threadMap = ThreadLocal.withInitial(() -> -1);
    static HashSet<Integer> threadSet;
    static boolean centralStopped;
    static final Object centralReady = new Object();
    static CoopThreadStatus threadStatus[];
    static CoopQuantum schedule[];
    public static int verbose = 1;

    static List theLog;

    public static void reset(int maxT) {
        maxThreads = maxT;
        numThreads = 0;
        threadMap = ThreadLocal.withInitial(() -> -1);
        threadSet = new HashSet<>();
        centralStopped = false;
        threadStatus = new CoopThreadStatus[maxThreads];
        for (int t = 0; t < maxThreads; t++) {
            threadStatus[t] = new CoopThreadStatus();
        }
        schedule = null;
        theLog = Collections.synchronizedList(new ArrayList<Object>());
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
                if (threadSet.contains(t)) {
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
            threadSet.add(t);
            // Don't set ready[t] here.  Instead, do it near top of sched().
            return t;
        }
    }

    public static void threadDone() {
        threadDone(threadMap.get());
    }

    public static void threadDone(int t) {
        try {
            threadStatus[t].done = true;
            synchronized (threadStatus[t]) {
                threadStatus[t].notify();
            }
        } catch (Exception e) {
            System.err.printf("ERROR: threadDone() exception %s\n", e.toString());
            return;
        }
    }

    public static void setSchedule(int schedIn[]) {
        schedule = new CoopQuantum[schedIn.length];
        for (int i = 0; i < schedIn.length; i++) {
            schedule[i] = new CoopQuantum(schedIn[i], 1);
        }
        addMissingThreadsToSchedule();
    }

    public static void setSchedule(CoopQuantum[] schedIn) {
        schedule = schedIn;
        addMissingThreadsToSchedule();
    }

    public static void setSchedule(List<CoopQuantum> schedIn) {
        schedule = schedIn.toArray(new CoopQuantum[0]);
        addMissingThreadsToSchedule();
    }

    private static void addMissingThreadsToSchedule() {
        HashSet<Integer> present = new HashSet<>();
        ArrayList<Integer> missing = new ArrayList<>();
        int numMissing = 0;

        for (int i = 0; i < schedule.length; i++) {
            present.add(schedule[i].thread);
        }
        for (int i = 0; i < maxThreads; i++) {
            if (! present.contains(i)) {
                missing.add(i);
                numMissing++;
            }
        }
        missing.sort(Comparator.naturalOrder());

        CoopQuantum sched[] = Arrays.copyOf(schedule, schedule.length + numMissing);
        for (int i = 0; i < numMissing; i++) {
            sched[schedule.length + i] = new CoopQuantum(missing.get(i), 1);
        }
        schedule = sched;
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
        if (threadStatus[t].done) {
            System.err.printf("ERROR: thread has called threadDone()!\n");
            return;
        }
        try {
            synchronized (threadStatus[t]) {
                if (threadStatus[t].ticks == 0) {
                    // If this is our first call ticks sched() after registerThread(), do not check tick balance.
                } else {
                    threadStatus[t].ticks--;
                    if (threadStatus[t].ticks > 0) {
                        // We still have a tick remaining, don't pester scheduler yet.
System.err.printf("SHOULD NOT HAPPEN TO t=%d\n", t);
                        return;
                    }
                }
                threadStatus[t].ready = true;
                threadStatus[t].notify();
            }
            synchronized (threadStatus[t]) {
                while (!centralStopped && threadStatus[t].ticks == 0) {
                    threadStatus[t].wait();
                }
                if (centralStopped) {
                    System.err.printf("NOTICE scheduler stopped, I am %d\n", t); return;
                }
            }
        } catch (InterruptedException e) {
            System.err.printf("sched nterrupted, TODO?\n");
            sched(t);
        }
    }

    public static void sched(boolean useCoopSched) {
        if (useCoopSched) {
            sched();
        }
    }

    public static void withdraw() {
        try {
            withdraw(threadMap.get());
        } catch (Exception e) {
            if (schedErrors++ < 3) {
                System.err.printf("ERROR: withdraw() exception %s\n", e.toString());
            } else if (schedErrors < 5) {
                System.err.printf("withdraw() exception warnings suppressed\n");
            }
            return;
        }
    }

    public static void withdraw(int t) throws Exception {
        if (t < 0 || t >= maxThreads) {
            throw new Exception("Bogus thread number " + t);
        }
        synchronized (threadStatus[t]) {
            threadStatus[t].ticks = 0;
            threadStatus[t].ready = false;
            threadStatus[t].notify();
        }
    }

    public static void rejoin() {
        try {
            rejoin(threadMap.get());
        } catch (Exception e) {
            if (schedErrors++ < 3) {
                System.err.printf("ERROR: rejoin() exception %s\n", e.toString());
            } else if (schedErrors < 5) {
                System.err.printf("rejoin() exception warnings suppressed\n");
            }
            return;
        }
    }

    public static void rejoin(int t) throws Exception {
        if (t < 0 || t >= maxThreads) {
            throw new Exception("Bogus thread number " + t);
        }
        threadStatus[t].ready = true;
    }

    public static void runScheduler(int numStartingThreads) {
        int t;
        int given = 0;

        while (! someReady(numStartingThreads)) {
            try { System.err.printf("!someReady,"); Thread.sleep(1); } catch (Exception e) {}
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
                t = schedule[i].thread;
                if (! threadStatus[t].ready || threadStatus[t].done) { if (verbose > 1) { log.info("SCHED-skipA {} ready {} done {}", t, threadStatus[t].ready, threadStatus[t].done); } continue; }
                if (schedule[i].ticks < 1) {
                    System.err.printf("TODO FIX ME Bad ticks value in schedule item %d: %d ticks\n", i, schedule[i].ticks);
                    return;
                }
                synchronized (threadStatus[t]) {
                    if (! threadStatus[t].ready || threadStatus[t].done) { if (verbose > 1) { log.info("SCHED-skipB {} ready {} done {}", t, threadStatus[t].ready, threadStatus[t].done); } continue; }
                    while (threadStatus[t].ticks != 0) {
                        try {threadStatus[t].wait(); if (verbose > 1) { log.info("SCHED-WAIT1 {}", t); } } catch (InterruptedException e) { System.err.printf("TODO BUMMER FIX ME\n"); return; }
                    }
                    threadStatus[t].ticks = schedule[i].ticks;
                    { if (verbose > 0) { log.info("SCHED-NOTIFY {}", t); } }
                    threadStatus[t].notify();
                    given++;
                }

                synchronized (threadStatus[t]) {
                    while (!threadStatus[t].done && threadStatus[t].ticks != 0) {
                        try {threadStatus[t].wait(); if (verbose > 1) { log.info("SCHED-WAIT2 {}", t); } } catch (InterruptedException e) { System.err.printf("TODO BUMMER FIX ME\n"); return; }
                    }
                }
            }
        }
        // System.err.printf("GIVEN = %d,", given);
    }

    private static boolean someReady(int maxThr) {
        for (int t = 0; t < maxThr; t++) {
            if (! threadStatus[t].ready) {
                return false;
            }
        }
        return true;
    }

    private static boolean allDone() {
        for (int t = 0; t < numThreads; t++) {
            if (! threadStatus[t].done) {
                return false;
            }
        }
        return true;
    }

    public static void appendLog(Object s) {
        synchronized (log) {
            theLog.add(s);
        }
    }

    public static Object[] getLog() {
        synchronized (log) {
            return theLog.toArray();
        }
    }

    public static boolean logsAreIdentical(ArrayList<Object[]> logs) {
        for (int i = 0; i < logs.size() - 2; i++) {
            Object[] l1 = logs.get(i);
            Object[] l2 = logs.get(i+1);
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
        boolean winnerPossible = RandomUtils.nextInt(100) < winnerThisTime;
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
