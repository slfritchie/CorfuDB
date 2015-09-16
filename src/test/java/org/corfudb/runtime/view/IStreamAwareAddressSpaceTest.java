package org.corfudb.runtime.view;

import org.corfudb.runtime.OverwriteException;
import org.corfudb.runtime.SubLogException;
import org.corfudb.runtime.UnwrittenException;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.UUID;

import static com.github.marschall.junitlambda.LambdaAssert.assertRaises;
import static org.junit.Assert.assertEquals;

/**
 * Created by taia on 9/15/15.
 */
public abstract class IStreamAwareAddressSpaceTest {

    IStreamAwareAddressSpace addressSpace;
    UUID stream = UUID.randomUUID();

    protected abstract IStreamAwareAddressSpace getAddressSpace();

    @Before
    public void setupAddressSpace()
    {
        addressSpace = getAddressSpace();
    }

    @Test
    public void AddressSpaceIsWritable() throws Exception
    {
        String testString = "hello world";
        addressSpace.write(0, Collections.singletonMap(stream, 0L), testString);
    }

    @Test
    public void AddressSpaceIsWriteOnce() throws Exception
    {
        String testString = "hello world";
        String testString2 = "hello world2";
        addressSpace.write(0,  Collections.singletonMap(stream, 0L), testString);
        assertRaises(() -> addressSpace.write(0,  Collections.singletonMap(stream, 0L), testString2), OverwriteException.class);
    }

    @Test
    public void AddressSpaceIsReadable() throws Exception
    {
        String testString = "hello world";
        addressSpace.write(0, Collections.singletonMap(stream, 0L), testString);
        assertEquals(addressSpace.readObject(0), testString);
    }

    @Test
    public void AddressSpaceReturnsUnwritten() throws Exception
    {
        assertRaises(() -> addressSpace.read(1), UnwrittenException.class);
        assertRaises(() -> addressSpace.read(1, stream), UnwrittenException.class);
    }

    @Test
    public void AddressSpaceViolateSublog() throws Exception {
        String testString = "hello world";
        for (int i = 0; i < 10; i++) {
            addressSpace.write((long) 2*i, Collections.singletonMap(stream, (long)i), testString);
        }
        assertRaises(() -> addressSpace.write(13L, Collections.singletonMap(stream, 11L), testString), SubLogException.class);
        addressSpace.write(30L, Collections.singletonMap(stream, 15l), testString);
        assertRaises(() -> addressSpace.write(40L, Collections.singletonMap(stream, 11L), testString), SubLogException.class);

    }
}
