package org.corfudb.runtime.protocols.replications;

import org.corfudb.infrastructure.NettyLogUnitServer;
import org.corfudb.infrastructure.SimpleLogUnitServer;
import org.corfudb.infrastructure.StreamingSequencerServer;
import org.corfudb.runtime.CorfuDBRuntime;
import org.corfudb.runtime.SubLogException;
import org.corfudb.runtime.UnwrittenException;
import org.corfudb.runtime.protocols.logunits.INewWriteOnceLogUnit;
import org.corfudb.runtime.protocols.logunits.IWriteOnceLogUnit;
import org.corfudb.runtime.view.StreamAwareAddressSpace;
import org.corfudb.util.CorfuInfrastructureBuilder;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static com.github.marschall.junitlambda.LambdaAssert.assertRaises;
import static org.junit.Assert.assertEquals;

/**
 * Created by taia on 9/16/15.
 */
public class NettyMultiReplicationProtocolIT {
    private static CorfuInfrastructureBuilder infrastructure;
    private static CorfuDBRuntime cdr;
    UUID stream = UUID.randomUUID();

    @BeforeClass
    public static void setInfrastructure() {
        infrastructure =
                CorfuInfrastructureBuilder.getBuilder()
                        .setReplicationProtocol("cdbnmrp")
                        .addSequencer(9001, StreamingSequencerServer.class, "cdbsts", null)
                        .addSALoggingUnit(9000, 0, NettyLogUnitServer.class, "nlu", null, false)
                        .addSALoggingUnit(9004, 0, NettyLogUnitServer.class, "nlu", null, false)
                        .addSALoggingUnit(9003, 1, NettyLogUnitServer.class, "nlu", null, true)
                        .addSALoggingUnit(9005, 1, NettyLogUnitServer.class, "nlu", null, true)
                        .start(9002);
        cdr = CorfuDBRuntime.createRuntime(infrastructure.getConfigString());
        cdr.waitForViewReady();
        cdr.getLocalInstance().getConfigurationMaster().resetAll();
    }

    @Before
    public void resetAll() {
        cdr.getLocalInstance().getConfigurationMaster().resetAll();
    }

    @AfterClass
    public static void shutdown() {
        infrastructure.shutdownAndWait();
    }

    @Test
    public void TestMultiIndexReplication() throws Exception {
        // Create an address space to make it easy to write things..
        StreamAwareAddressSpace as = new StreamAwareAddressSpace(cdr);
        String testString = "hello world";
        as.write(0L, Collections.singletonMap(stream, 0L), testString);
        assertEquals(as.readObject(0L), testString);
        assertEquals(as.readObject(0L, stream), testString);

        // Now unset some commit bits
        INewWriteOnceLogUnit one = (INewWriteOnceLogUnit) cdr.getView().getSegments().get(0).getGroups().get(0).get(0);
        one.setCommit(0L, false);
        assertRaises(() -> as.read(0L), UnwrittenException.class);
        assertRaises(() -> as.readObject(0L), UnwrittenException.class);

        one.setCommit(0L, true);
        assertEquals(as.readObject(0L), testString);

        INewWriteOnceLogUnit second;
        if ((stream.hashCode() % 2) == 0)
            second = (INewWriteOnceLogUnit) cdr.getView().getSegments().get(0).getGroups().get(1).get(0);
        else second = (INewWriteOnceLogUnit) cdr.getView().getSegments().get(0).getGroups().get(1).get(1);

        second.setCommit(Collections.singletonMap(stream, 0L), false).get();
        assertRaises(() -> as.read(0L, stream), UnwrittenException.class);
        assertRaises(() -> as.readObject(0L, stream), UnwrittenException.class);

        second.setCommit(Collections.singletonMap(stream, 0L), true).get();
        assertEquals(as.readObject(0L, stream), testString);
    }

    @Test
    public void TestSublogInvariant() throws Exception {
        StreamAwareAddressSpace as = new StreamAwareAddressSpace(cdr);
        String testString = "hello world";
        as.write(0L, Collections.singletonMap(stream, 0L), testString);
        as.write(100L, Collections.singletonMap(stream, 100L), testString);

        assertRaises(() -> as.write(42L, Collections.singletonMap(stream, 101L), testString), SubLogException.class);

        as.write(109L, Collections.singletonMap(stream, 110L), testString);

        assertRaises(() -> as.write(110L, Collections.singletonMap(stream, 105L), testString), SubLogException.class);
        assertRaises(() -> as.write(105L, Collections.singletonMap(stream, 111L), testString), SubLogException.class);
    }
}
