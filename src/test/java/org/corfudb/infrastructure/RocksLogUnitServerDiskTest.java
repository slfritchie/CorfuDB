package org.corfudb.infrastructure;

import org.corfudb.infrastructure.thrift.*;
import org.corfudb.runtime.protocols.logunits.CorfuDBRocksLogUnitProtocol;
import org.corfudb.runtime.protocols.logunits.IStreamAwareLogUnit;
import org.corfudb.runtime.protocols.logunits.IWriteOnceLogUnit;
import org.corfudb.util.Utils;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.UUID;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class RocksLogUnitServerDiskTest {

    private static byte[] getTestPayload(int size)
    {
        byte[] test = new byte[size];
        for (int i = 0; i < size; i++)
        {
            test[i] = (byte)(i % 255);
        }
        return test;
    }
    private static RocksLogUnitServer slus = new RocksLogUnitServer();

    private static String TESTFILE = "testFile";
    private static int PAGESIZE = 4096;
    private static org.corfudb.infrastructure.thrift.UUID uuid = Utils.toThriftUUID(UUID.randomUUID());

    private static ByteBuffer test = ByteBuffer.wrap(getTestPayload(PAGESIZE));
    private static ArrayList<Integer> epochlist = new ArrayList<Integer>();

    private void deleteFile(File file) {
        if (file.isDirectory()) {
            File[] children = file.listFiles();
            for (File f : children)
                deleteFile(f);
        }
        file.delete();
    }

    @BeforeClass
    public static void setupServer() throws Exception {
        HashMap<String, Object> configMap = new HashMap<String, Object>();
        configMap.put("ramdisk", false);
        configMap.put("capacity", 1000);
        configMap.put("port", 0);
        configMap.put("pagesize", PAGESIZE);
        configMap.put("trim", -1);
        configMap.put("drive", TESTFILE);
        Thread t = new Thread(slus.getInstance(configMap));
        t.start();

        epochlist.add(0);

        // Wait for server thread to finish setting up
        boolean done = false;

        while (!done) {
            try {
                slus.read(new StreamUnitServerHdr(epochlist, 0, Collections.singletonMap(uuid, 0L)));
                done = true;
            } catch (Exception e) {}
        }

        // Write entries in for the tests
        for (int i = 0; i < 100; i++)
        {
            test.position(0);
            ErrorCode ec = slus.write(new StreamUnitServerHdr(epochlist, i, Collections.singletonMap(uuid, Long.valueOf((long)i))), test, ExtntMarkType.EX_FILLED).getCode();
            assertEquals(ec, ErrorCode.OK);
        }
    }

    @After
    public void tearDown() {
        File file = new File(TESTFILE);
        deleteFile(file);
    }

    @Test
    public void checkIfLogUnitIsWriteOnce() throws Exception
    {
        ErrorCode ec = slus.write(new StreamUnitServerHdr(epochlist, 42, Collections.singletonMap(uuid, 42L)), test, ExtntMarkType.EX_FILLED).getCode();
        assertEquals(ErrorCode.ERR_OVERWRITE, ec);
    }


    @Test
    public void checkIfLogIsReadable() throws Exception
    {
        ExtntWrap ew = slus.read(new StreamUnitServerHdr(epochlist, -1L, Collections.singletonMap(uuid, 0L)));
        //ew.getCtnt().get(0).position(0);
        test.position(0);
        assert(test.equals(ew.getCtnt().get(0)));
    }

    @Test
    public void checkIfEmptyAddressesAreUnwritten() throws Exception
    {
        ExtntWrap ew = slus.read(new StreamUnitServerHdr(epochlist, -1L, Collections.singletonMap(uuid, 101L)));
        assertEquals(ew.getErr(), ErrorCode.ERR_UNWRITTEN);
    }

    @Test
    public void checkCommitBit() throws Exception {
        ErrorCode ec = slus.setCommit(new StreamUnitServerHdr(epochlist, -1L, Collections.singletonMap(uuid, 10L)), true);
        assertEquals(ec, ErrorCode.OK);

        ExtntWrap ew = slus.read(new StreamUnitServerHdr(epochlist, -1L, Collections.singletonMap(uuid, 10L)));
        assert(ew.getInf().isCommit());
        assertEquals(ew.getInf().getFlag(), ExtntMarkType.EX_FILLED);

        ew.getCtnt().get(0).position(0);
        test.position(0);
        assert(test.equals(ew.getCtnt().get(0)));
    }

    @Test
    public void checkConsensusDecision() throws Exception {
        ErrorCode ec = slus.write(new StreamUnitServerHdr(epochlist, 42, Collections.singletonMap(uuid, 101L)), test, ExtntMarkType.EX_FILLED).getCode();
        assertEquals(ErrorCode.ERR_SUBLOG, ec);

        ec = slus.write(new StreamUnitServerHdr(epochlist, 109, Collections.singletonMap(uuid, 110L)), test, ExtntMarkType.EX_FILLED).getCode();
        assertEquals(ErrorCode.OK, ec);
        ec = slus.write(new StreamUnitServerHdr(epochlist, 110, Collections.singletonMap(uuid, 105L)), test, ExtntMarkType.EX_FILLED).getCode();
        assertEquals(ErrorCode.ERR_SUBLOG, ec);
        ec = slus.write(new StreamUnitServerHdr(epochlist, 105, Collections.singletonMap(uuid, 111L)), test, ExtntMarkType.EX_FILLED).getCode();
        assertEquals(ErrorCode.ERR_SUBLOG, ec);
    }
}
