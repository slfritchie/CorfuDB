package org.corfudb.util;

import groovy.transform.ASTTest;

import java.util.LinkedList;
import java.util.List;

import static org.corfudb.util.CoopScheduler.sched;

/**
 *  Just barely enough of a StampedLock API to try to use with
 *  Corfu's VersionLockedObject.  I've skimmed the StampedLock
 *  API docs a bit, what could possibly go wrong?
 */

public class CoopStampedLock {
    public long tstamp = 1;
    // private long numReaders = 0;
    // private List<Integer> waitingReaders = new LinkedList<>();
    public long writer = -1;
    public List<Integer> waitingWriters = new LinkedList<>();

    public CoopStampedLock() {
        // TODO?
    }

    public long tryOptimisticRead() {
        if (CoopScheduler.threadMap.get() < 0) {
            System.err.printf("ERROR: tryOptimisticRead() by non-coop thread\n");
            return 0;
        }
        if (writer >= 0) {
            return 0;
        } else {
            return tstamp;
        }
    }

    public boolean validate(long tstamp) {
        if (tstamp == 0) {
            return false;
        }
        return tstamp == this.tstamp;
    }

    public long tryConvertToWriteLock(long tstamp) {
        int t = CoopScheduler.threadMap.get();
        if (t < 0) {
            System.err.printf("ERROR: tryConvertToWriteLock() by non-coop thread\n");
            return 0;
        }
        if (tstamp == 0 || tstamp != this.tstamp) {
            return 0;
        }
        return writeLock();
    }

    public long writeLock() {
        int t = CoopScheduler.threadMap.get();
        if (t < 0) {
            System.err.printf("ERROR: tryConvertToWriteLock() by non-coop thread\n");
            return 0;
        }
        while (writer >= 0) {
            sched();
        }
        tstamp++;
        writer = t;
        return tstamp;
    }

    public void unlock(long tstamp) {
        int t = CoopScheduler.threadMap.get();
        if (t < 0) {
            System.err.printf("ERROR: unlock() by non-coop thread\n");
            return;
        }
        if (tstamp == 0 || tstamp != this.tstamp) {
            System.err.printf("ERROR: unlock() with bogus tstamp: %d != correct value %d\n", tstamp, this.tstamp);
            return;
        }
        if (t != writer) {
            System.err.printf("ERROR: unlock() with correct tstamp: %d but caller tid %d != correct tid %d\n",
                    this.tstamp, t, writer);
            return;
        }
        tstamp++;
        writer = -1;
    }


}
