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

    /*********
    @Before("set(org.corfudb.runtime.object.transactions.WriteSetSMRStream org.corfudb.runtime.object.VersionLockedObject.optimisticStream) && "
            + "args(s)")
    public void sched_oSA0(org.corfudb.runtime.object.transactions.WriteSetSMRStream s, JoinPoint tjp) {
        //System.err.printf("%s,", s);
        sched();
    }
      *********/


    /* Get of 'optimisticStream'.  Note that this applies even to using optimisticStream
     * as an argument to a method call.
     */
    /*********
    @Before("get(org.corfudb.runtime.object.transactions.WriteSetSMRStream org.corfudb.runtime.object.VersionLockedObject.optimisticStream)")
    public void sched_oSG0() {
        //System.err.printf("%s,", s);
        sched();
    }
     *********/

    @After("call(* org.corfudb.runtime.object.VersionLockedObject.getOptimisticStreamUnsafe(..))")
    public void sched_oSG0() {
        //System.err.printf("%s,", Thread.currentThread().getName());
        sched();
    }

}
