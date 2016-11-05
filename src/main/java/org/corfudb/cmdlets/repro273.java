package org.corfudb.cmdlets;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import org.codehaus.plexus.util.ExceptionUtils;
import org.corfudb.infrastructure.CorfuServer;
import org.corfudb.infrastructure.LogUnitServer;
import org.corfudb.infrastructure.SequencerServer;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.util.GitRepositoryState;
import org.corfudb.util.Utils;
import org.docopt.Docopt;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import lombok.extern.slf4j.Slf4j;
import org.slf4j.LoggerFactory;


/**
 * Created by mwei on 1/21/16.
 */
@Slf4j
public class repro273 {

    static CorfuRuntime rt1, rt2;
    static Object o1, o2;
    static Method mPut, mGet;

    public static void main(String[] args) {

         String USAGE =
                "Reproduce bug 273.\n"
                        + "\n"
                        + "Usage:\n"
                        + "\tcorfu_repro273 -c <config> [-d <level>]\n"
                        + "\n"
                        + "Options:\n"
                        + " -c <config>, --config=<config>                 The config string to pass to the org.corfudb.runtime. \n"
                        + "                                                Usually a comma-delimited list of layout servers.\n"
                        + " -d <level>, --log-level=<level>                Set the logging level, valid levels are: \n"
                        + "                                                ERROR,WARN,INFO,DEBUG,TRACE [default: INFO].\n"
                        + " --version                                      Show version\n";

        Map<String, Object> opts =
                new Docopt(USAGE).withVersion(GitRepositoryState.getRepositoryState().describe).parse(args);
        String config = (String) opts.get("--config");
        configureBase(opts);

        rt1 = configureRuntime(opts);
        rt2 = configureRuntime(opts);

        // Attempt to open the object
        Class<?> cls;
        try {
            cls = Class.forName("org.corfudb.runtime.collections.SMRMap");
        } catch (ClassNotFoundException cnfe) {
            throw new RuntimeException(cnfe);
        }

        o1 = rt1.getObjectsView().build()
                .setStreamName((String) "42")
                .setType(cls)
                .open();
        o2 = rt2.getObjectsView().build()
                .setStreamName((String) "42")
                .setType(cls)
                .open();

        // Use reflection to find the method...
        try {
            int putArity = 2;
            mPut = Arrays.stream(cls.getDeclaredMethods())
                    .filter(x -> x.getName().equals("put"))
                    .filter(x -> x.getParameterCount() == putArity)
                    .findFirst().get();
            int getArity = 1;
            mGet = Arrays.stream(cls.getDeclaredMethods())
                    .filter(x -> x.getName().equals("get"))
                    .filter(x -> x.getParameterCount() == getArity)
                    .findFirst().get();
        } catch (NoSuchElementException nsee) {
            System.err.println("Error 1");
            return;
        }
        if (mPut == null || mGet == null) {
            System.err.println("Error 1");
            return;
        }

        Thread thr1 = new Thread(repro273::t1Func);
        Thread thr2 = new Thread(repro273::t2Func);
        thr1.start();
        thr2.start();

        try { thr2.join(); } catch (Exception e) { System.out.printf("thr2 ex\n"); }
        try { thr1.join(); } catch (Exception e) { System.out.printf("thr1 ex\n"); }
    }

    static void configureBase(Map<String, Object> opts) {
        Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        System.out.println("configureBase opts = " + opts.toString());
        switch ((String) opts.get("--log-level")) {
            case "ERROR":
                root.setLevel(Level.ERROR);
                break;
            case "WARN":
                root.setLevel(Level.WARN);
                break;
            case "INFO":
                root.setLevel(Level.INFO);
                break;
            case "DEBUG":
                root.setLevel(Level.DEBUG);
                break;
            case "TRACE":
                root.setLevel(Level.TRACE);
                break;
            default:
                root.setLevel(Level.INFO);
                System.out.println("Level " + opts.get("--log-level") + " not recognized, defaulting to level INFO");
        }
    }

    static CorfuRuntime configureRuntime(Map<String, Object> opts) {
        return new CorfuRuntime()
                .parseConfigurationString((String) opts.get("--config"))
                .connect();
    }

    /* Objects o1 & o2 are based on separate CorfuRuntime instances */

    static void t1Func() {
        Object ret = null;

        for (int i = 0; i < 10000; i++) {
            try {
                String a1[] = {"a"};
                ret = mGet.invoke(o1, a1);
                System.out.printf("1");
                String a2[] = {"l", "Hello-world!"};
                ret = mPut.invoke(o1, a2);
                System.out.printf("2");
                String a3[] = {"v"};
                ret = mGet.invoke(o1, a3);
                System.out.printf("3");
                String a4[] = {"l", "Another-value"};
                ret = mPut.invoke(o1, a4);
                System.out.printf("4");
            } catch (Exception e) {
                System.err.println("Exception on object: " + e + "\n" +
                        "stack: " + ExceptionUtils.getStackTrace(e) + "\n" +
                        "cause: " + ExceptionUtils.getCause(e));
            }
        }

    }

    static void t2Func() {
        Object ret = null;

        for (int i = 0; i < 10000; i++) {
            try {
                String a5[] = {"n", ""};
                ret = mPut.invoke(o2, a5);
                System.out.printf("5");
                String a6[] = {"a", "Hello-world!"};
                ret = mPut.invoke(o2, a6);
                System.out.printf("6");
            } catch (Exception e) {
                System.out.println("Exception on object: " + e + "\n" +
                        "stack: " + ExceptionUtils.getStackTrace(e) + "\n" +
                        "cause: " + ExceptionUtils.getCause(e));
            }
        }
    }
}
