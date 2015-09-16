package org.corfudb.runtime.view;

import org.corfudb.infrastructure.RocksLogUnitServer;
import org.corfudb.infrastructure.SimpleLogUnitServer;
import org.corfudb.infrastructure.StreamingSequencerServer;
import org.corfudb.runtime.CorfuDBRuntime;
import org.corfudb.util.CorfuInfrastructureBuilder;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by taia on 9/15/15.
 */
public class StreamAwareAddressSpaceIT extends IStreamAwareAddressSpaceTest {
    private static CorfuInfrastructureBuilder infrastructure;

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
            put("drive", "saTestFile");
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
    }

    @Override
    public IStreamAwareAddressSpace getAddressSpace()
    {
        CorfuDBRuntime cdr = CorfuDBRuntime.createRuntime(infrastructure.getConfigString());
        cdr.waitForViewReady();
        cdr.getLocalInstance().getConfigurationMaster().resetAll();
        return new StreamAwareAddressSpace(cdr);
    }

    @AfterClass
    public static void shutdown() {
        infrastructure.shutdownAndWait();
    }
}
