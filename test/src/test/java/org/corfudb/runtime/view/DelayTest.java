package org.corfudb.runtime.view;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.math.RandomUtils;
import org.corfudb.util.CoopScheduler;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by mwei on 1/6/16.
 */
@Slf4j
public class DelayTest  {
    @Test
    @SuppressWarnings("checkstyle:magicnumber")
    public void t0() throws Exception {
        ArrayList<String[]> logs = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            logs.add(t0Inner());
        }
        assertThat(CoopScheduler.logsAreIdentical(logs)).isTrue();
    }

    @SuppressWarnings("checkstyle:magicnumber")
    public String[] t0Inner() throws Exception {
        int[] schedule = new int[] {1,1,0,2,1,0,4,3};
        CoopScheduler.reset(schedule.length + 4); // add a few unused thread count for bug hunting
        CoopScheduler.setSchedule(schedule);

        Thread t[] = new Thread[] {
                new Thread(() -> { threadWork(RandomUtils.nextInt(30)); }),
                new Thread(() -> { threadWork(RandomUtils.nextInt(30)); }),
                new Thread(() -> { threadWork(RandomUtils.nextInt(30)); }),
                new Thread(() -> { threadWork(RandomUtils.nextInt(30)); }),
                new Thread(() -> { threadWork(RandomUtils.nextInt(30)); })
        };
        for (int i = 0; i < t.length; i++) { t[i].start(); }
        CoopScheduler.runScheduler(t.length);
        for (int i = 0; i < t.length; i++) { t[i].join(); }

        return CoopScheduler.getLog();
    }

    @SuppressWarnings("checkstyle:magicnumber")
    void threadWork(int sleepTime) {
        try { Thread.sleep(sleepTime); } catch (Exception e) {}
        int t = CoopScheduler.registerThread();
        if (t < 0) {
            System.err.printf("Thread registration failed, exiting");
            System.exit(1);
        }

        for (int i = 0; i < 5; i++) {
            CoopScheduler.sched();
            CoopScheduler.appendLog("T " + t + " i " + i);
            // System.err.printf("Thread %d iter (%d)", t, i);
            try { Thread.sleep(sleepTime); } catch (Exception e) {}
        }

        CoopScheduler.threadDone();
    }
}
