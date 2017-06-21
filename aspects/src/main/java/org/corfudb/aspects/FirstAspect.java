package org.corfudb.aspects;

import lombok.Getter;
import lombok.Setter;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;

@Aspect
public class FirstAspect {
    @Getter
    @Setter
    private boolean enabled = false;
    private boolean warned = false;

    @Around("execution(* java.lang.Thread.sleep(..))")
    public Object advice(ProceedingJoinPoint pjp) throws Throwable {
        System.err.printf("sleep...");
        System.err.printf("sleep arg0 = %s\n", pjp.getArgs()[0]);
        Object res = pjp.proceed();
        return res;
    }

    @Around("execution(* Thread.sleep(..))")
    public Object advice_bareSleep(ProceedingJoinPoint pjp) throws Throwable {
        System.err.printf("sleep...");
        System.err.printf("sleep arg0 = %s\n", pjp.getArgs()[0]);
        Object res = pjp.proceed();
        return res;
    }

    @Around("execution(* org.corfudb.runtime.view.stream.BackpointerStreamView.fillReadQueue(..))")
    public Object advice_fillReadQueue(ProceedingJoinPoint pjp) throws Throwable {
        Object res = pjp.proceed();
        if (enabled) {
            System.err.printf("Hello, fillReadQueue, from aspect world!\n");
        } else {
            if (!warned) {
                System.err.printf("FirstAspect is disabled.\n");
                System.err.printf("pjp = %s\n", pjp);
                System.err.printf("pjp long = %s\n", pjp.toLongString());
                System.err.printf("pjp arg0 = %s\n", pjp.getArgs()[0]);
                System.err.printf("pjp arg1 = %s\n", pjp.getArgs()[1]);
                System.err.printf("pjp target = %s\n", pjp.getTarget());
                System.err.printf("pjp kind = %s\n", pjp.getKind());
                warned = true;
            }
        }
        return res;
    }

    private int readCount = 0;
    @Around("execution(* org.corfudb.runtime.view.AddressSpaceView.read(..))")
    public Object advice_ASV_read(ProceedingJoinPoint pjp) throws Throwable {
        if (readCount++ < 42) { System.err.printf("r"); }
        Object res = pjp.proceed();
        return res;
    }

}

