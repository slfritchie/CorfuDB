package org.corfudb.util;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static org.corfudb.util.CoopScheduler.sched;

public class CoopUtil {
    public List<Callable> scheduledThreads = new ArrayList<>();

    /**
     * Schedule a task to run concurrently when executeScheduled() is called.
     *
     * @param function The function to run.
     */
    public void scheduleCoopConcurrently(BiConsumer function) {
        scheduleCoopConcurrently(1, function);
    }

    /**
     * Schedule a task to run concurrently when executeScheduled() is called multiple times.
     *
     * @param repetitions The number of times to repeat execution of the function.
     * @param function    The function to run.
     */
    public void scheduleCoopConcurrently(int repetitions, BiConsumer function) {
        int threadBase = scheduledThreads.size();

        for (int i = 0; i < repetitions; i++) {
            final int thr = i + threadBase;
            final int ii = i;

            scheduledThreads.add(() -> {
                CoopScheduler.registerThread(thr);
                sched();
                function.accept(thr, ii);
                CoopScheduler.threadDone();
                return null;
            });
        }
    }

    /**
     * Execute any threads which were scheduled to run.
     */
    public void executeScheduled() throws Exception {
        executeScheduled(null);
    }

    public void executeScheduled(Consumer exceptionLambda) throws Exception {
        Thread ts[] = new Thread[scheduledThreads.size()];

        for (int i = 0; i < scheduledThreads.size(); i++) {
            final int ii = i;
            ts[i] = new Thread(() -> {
                Thread.currentThread().setName("coop-thr-" + ii);
                try {
                    scheduledThreads.get(ii).call();
                } catch (Exception e) {
                    if (exceptionLambda == null) {
                        System.err.printf("executeScheduled error by thr %d: %s\n", ii, e);
                    } else {
                        exceptionLambda.accept(e);
                    }
                    CoopScheduler.threadDone();
                }
            });
            ts[i].start();
        }
        CoopScheduler.runScheduler(scheduledThreads.size());
        for (int i = 0; i < scheduledThreads.size(); i++) {
            ts[i].join();
        }
    }

    public static void barrierCountdown(AtomicInteger barrier) {
        barrier.getAndIncrement();
    }

    public static void barrierAwait(AtomicInteger barrier, int max) {
        while (barrier.get() < max) {
            sched();
        }
    }

    public static void lock(AtomicInteger lock) {
        while (lock.get() != 0) {
            sched();
        }
        lock.set(1);
    }

    public static void unlock(AtomicInteger lock) {
        lock.set(0);
    }

    public static void await(AtomicInteger lock, AtomicInteger cond) {
        while (cond.get() == 0) {
            unlock(lock);
            sched();
            lock(lock);
        }
        sched();
        cond.set(0);
    }
}
