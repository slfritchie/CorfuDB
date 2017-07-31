package org.corfudb.generator;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.corfudb.generator.operations.BaseOperation;
import org.corfudb.generator.operations.CheckpointOperation;
import org.corfudb.generator.operations.Operation;
import org.corfudb.runtime.CorfuRuntime;

import static java.util.concurrent.Executors.newWorkStealingPool;

/**
 * Generator is a program that generates synthetic workloads that try to mimic
 * real applications that consume the CorfuDb client. Application's access
 * patterns can be approximated by setting different configurations like
 * data distributions, concurrency level, operation types, time skews etc.
 *
 *
 * Created by maithem on 7/14/17.
 */
public class Generator {
    public static void main(String[] args) {
        String endPoint = args[0];
        int numStreams = Integer.parseInt(args[1]);
        int numKeys = Integer.parseInt(args[2]);
        long cpPeriod = Long.parseLong(args[3]);
        int numThreads = Integer.parseInt(args[4]);
        int numOperations = Integer.parseInt(args[5]);

        CorfuRuntime rt = new CorfuRuntime(endPoint).connect();

        State state = new State(numStreams, numKeys, rt);

        Runnable cpTrimTask = () -> {
            Operation op = new CheckpointOperation(state);
            op.execute();
        };

        final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        scheduler.scheduleAtFixedRate(cpTrimTask, 30, cpPeriod, TimeUnit.SECONDS);

        ExecutorService appWorkers = newWorkStealingPool(numThreads);

        Runnable app = () -> {
            List<Operation> operations = state.getOperations().sample(numOperations);
            for (Operation operation : operations) {
                try {
                    BaseOperation base = new BaseOperation(state, operation);
                    base.execute();
                } catch (Exception ex) {
                    System.err.printf(".");
                    // ex.printStackTrace();
                }
            }
        };

        Future[] appsFutures = new Future[numThreads];

        for (int x = 0; x < numThreads; x++) {
            final int xx = x;
            appsFutures[x] = appWorkers.submit(() -> {
                // Just a raw integer, in case Knossos doesn't like strings as process IDs
                Thread.currentThread().setName(String.format("%d", xx));
                app.run();
            });
        }

        for (int x = 0; x < numThreads; x++) {
            try {
                appsFutures[x].get();
                System.err.printf("; Thread %d finished\n", x);
            } catch (Exception e) {
                System.out.println("App Exception: " + e);
            }
        }
        System.exit(0);
    }
}
