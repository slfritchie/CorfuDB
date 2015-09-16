/**
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.corfudb.runtime.view;

import org.corfudb.runtime.*;
import org.corfudb.runtime.protocols.replications.IStreamAwareRepProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;

/**
 * This view implements a simple write once address space
 *
 * Created by taia on 9/15/15
 */

public class StreamAwareAddressSpace implements IStreamAwareAddressSpace {

    private CorfuDBRuntime client;
    private UUID logID;
    private CorfuDBView view;
    private Supplier<CorfuDBView> getView;

    private final Logger log = LoggerFactory.getLogger(StreamAwareAddressSpace.class);

    public StreamAwareAddressSpace(CorfuDBRuntime client)
    {
        this.client = client;
        this.getView = this.client::getView;
        this.logID = getView.get().getUUID();
    }

    public StreamAwareAddressSpace(CorfuDBRuntime client, UUID logID)
    {
        this.client = client;
        this.logID = logID;
        this.getView = () -> {
            try {
                return this.client.getView(this.logID);
            }
            catch (RemoteException re)
            {
                log.warn("Error getting remote view", re);
                return null;
            }
        };
    }

    public StreamAwareAddressSpace(CorfuDBView view)
    {
        this.view = view;
        this.getView = () -> {
            return this.view;
        };
    }

    @Override
    public void write(long address, Map<UUID, Long> streams, Serializable s)
            throws IOException, OverwriteException, TrimmedException, OutOfSpaceException, SubLogException
    {
        try (ByteArrayOutputStream bs = new ByteArrayOutputStream())
        {
            try (ObjectOutput out = new ObjectOutputStream(bs))
            {
                out.writeObject(s);
                write(address, streams, bs.toByteArray());
            }
        }

    }

    @Override
    public void write(long address, Map<UUID, Long> streams, byte[] data)
            throws OverwriteException, TrimmedException, OutOfSpaceException, SubLogException
    {
        CorfuDBViewSegment segments =  getView.get().getSegments().get(0);
        IStreamAwareRepProtocol replicationProtocol = segments.getStreamAwareRepProtocol();

        replicationProtocol.write(client, address, streams, data);
        return;
    }

    @Override
    public byte[] read(long physAddress)
            throws UnwrittenException, TrimmedException
    {
        //TODO: cache the layout so we don't have to determine it on every write.
        CorfuDBViewSegment segments =  getView.get().getSegments().get(0);
        IStreamAwareRepProtocol replicationProtocol = segments.getStreamAwareRepProtocol();

        if (logID != null)
            return replicationProtocol.read(client, physAddress, logID, -1L);
        return replicationProtocol.read(client, physAddress, null, -1L);
    }

    public Object readObject(long address)
            throws UnwrittenException, TrimmedException, ClassNotFoundException, IOException
    {
        byte[] payload = read(address);
        try (ByteArrayInputStream bis = new ByteArrayInputStream(payload))
        {
            try (ObjectInputStream ois = new ObjectInputStream(bis))
            {
                return ois.readObject();
            }
        }
    }

    @Override
    public byte[] read(long logAddress, UUID stream) throws UnwrittenException, TrimmedException {
        CorfuDBViewSegment segments =  getView.get().getSegments().get(0);
        IStreamAwareRepProtocol replicationProtocol = segments.getStreamAwareRepProtocol();

        if (logID != null)
            return replicationProtocol.read(client, -1L, logID, logAddress);
        return replicationProtocol.read(client, -1L, null, logAddress);

    }

    @Override
    public Object readObject(long logAddress, UUID stream)
            throws UnwrittenException, TrimmedException, ClassNotFoundException, IOException {
        byte[] payload = read(logAddress, stream);
        try (ByteArrayInputStream bis = new ByteArrayInputStream(payload))
        {
            try (ObjectInputStream ois = new ObjectInputStream(bis))
            {
                return ois.readObject();
            }
        }
    }
}


