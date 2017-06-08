package org.corfudb.util;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;

@Aspect
public class FirstAspect {
    private String message;

    public void setMessage(String message) {
        this.message = message;
    }

    @Around("execution(* org.corfudb.runtime.view.stream.BackpointerStreamView.fillReadQueue(..))")
    public Object advice(ProceedingJoinPoint pjp) throws Throwable {
        Object res = pjp.proceed();
        System.err.printf("Hello, fillReadQueue, from aspect world!\n");
        return res;
    }

}

