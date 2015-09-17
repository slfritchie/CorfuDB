package org.corfudb.runtime.protocols.replications;

import org.corfudb.infrastructure.RocksLogUnitServer;
import org.corfudb.infrastructure.SimpleLogUnitServer;
import org.corfudb.infrastructure.StreamingSequencerServer;
import org.corfudb.runtime.CorfuDBRuntime;
import org.corfudb.runtime.UnwrittenException;
import org.corfudb.runtime.protocols.logunits.IStreamAwareLogUnit;
import org.corfudb.runtime.protocols.logunits.IWriteOnceLogUnit;
import org.corfudb.runtime.view.StreamAwareAddressSpace;
import org.corfudb.util.CorfuInfrastructureBuilder;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static com.github.marschall.junitlambda.LambdaAssert.assertRaises;

/**
 * Created by taia on 9/16/15.
 */
public class MultiIndexReplicationProtocolIT {
    private static CorfuInfrastructureBuilder infrastructure;
    private static CorfuDBRuntime cdr;
    private static IStreamAwareRepProtocol mirp;
    UUID stream = UUID.randomUUID();

    static Map<String, Object> luConfigMap = new HashMap<String,Object>() {
        {
            put("capacity", 200000);
            put("ramdisk", true);
            put("pagesize", 4096);
            put("trim", 0);
        }
    };

    static Map<String, Object> ruConfigMap = new HashMap<String,Object>() {
        {
            put("capacity", 200000);
            put("ramdisk", false);
            put("pagesize", 4096);
            put("trim", 0);
            put("drive", "miTestFile");
        }
    };

    @BeforeClass
    public static void setInfrastructure() {
        infrastructure =
                CorfuInfrastructureBuilder.getBuilder()
                        .setReplicationProtocol("cdbmir")
                        .addSequencer(9001, StreamingSequencerServer.class, "cdbsts", null)
                        .addSALoggingUnit(9000, 0, SimpleLogUnitServer.class, "cdbslu", luConfigMap)
                        .addSALoggingUnit(9003, 1, RocksLogUnitServer.class, "rockslu", ruConfigMap)
                        .start(9002);
        cdr = CorfuDBRuntime.createRuntime(infrastructure.getConfigString());
        cdr.waitForViewReady();
        cdr.getLocalInstance().getConfigurationMaster().resetAll();

        mirp = cdr.getView().getSegments().get(0).getStreamAwareRepProtocol();
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
        IWriteOnceLogUnit one = (IWriteOnceLogUnit) cdr.getView().getSegments().get(0).getGroups().get(0).get(0);
        one.setCommit(0L, null, false);
        assertRaises(() -> as.read(0L), UnwrittenException.class);
        assertRaises(() -> as.readObject(0L), UnwrittenException.class);

        one.setCommit(0L, null, true);
        assertEquals(as.readObject(0L), testString);

        IStreamAwareLogUnit second = (IStreamAwareLogUnit) cdr.getView().getSegments().get(0).getGroups().get(1).get(0);
        second.setCommit(0L, stream, false);
        assertRaises(() -> as.read(0L, stream), UnwrittenException.class);
        assertRaises(() -> as.readObject(0L, stream), UnwrittenException.class);

        second.setCommit(0L, stream, true);
        assertEquals(as.readObject(0L, stream), testString);
    }
}
