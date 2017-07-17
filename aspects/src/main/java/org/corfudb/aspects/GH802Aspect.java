package org.corfudb.aspects;

import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.After;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import static org.corfudb.util.CoopScheduler.sched;

/**
 * Attempt to use AspectJ to reproduce the process scheduling
 * interleaving necessary to trigger the problem described by
 * GitHub issue #802.
 */

@Aspect
public class GH802Aspect {

    /* Direct assignment/set of 'optimisticStream' */

    @After("set(org.corfudb.runtime.object.transactions.WriteSetSMRStream org.corfudb.runtime.object.VersionLockedObject.optimisticStream)")
        public void sched_oSA0() {
        // System.err.printf("s");
        sched();
    }

    /* Get of 'optimisticStream'.  Note that this applies even to using optimisticStream
     * as an argument to a method call.
     */
    @After("get(org.corfudb.runtime.object.transactions.WriteSetSMRStream org.corfudb.runtime.object.VersionLockedObject.optimisticStream)")
    public void sched_oSG0() {
        // System.err.printf("g");
        sched();
    }

    @Before("call(* org.corfudb.runtime.object.VersionLockedObject.hack(..))")
    public void sched_oC0(JoinPoint tjp) {
        // System.err.printf("hV,");
        sched();
    }

    @Before("call(* org.corfudb.runtime.object.transactions.OptimisticTransactionalContext.hack(..))")
    public void sched_oC1(JoinPoint tjp) {
        // System.err.printf("hO,");
        sched();
    }

    /*****
    @After("call(* org.corfudb.runtime.object.VersionLockedObject.getOptimisticStreamUnsafe(..))")
    public void sched_oSG0() {
        //System.err.printf("%s,", Thread.currentThread().getName());
        sched();
    }
     *****/
}
