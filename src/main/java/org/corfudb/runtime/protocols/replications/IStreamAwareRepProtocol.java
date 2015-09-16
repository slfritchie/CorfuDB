package org.corfudb.runtime.protocols.replications;

import org.corfudb.runtime.*;
import org.corfudb.runtime.protocols.IServerProtocol;

import javax.json.JsonObject;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * A replication protocol that is stream-aware, i.e.- requires global and logical addresses.
 *
 * Created by taia on 8/4/15.
 */
public interface IStreamAwareRepProtocol {

    static String getProtocolString()
    {
        return "unknown";
    }

    static Map<String, Object> segmentParser(JsonObject jo) {
        throw new UnsupportedOperationException("This replication protocol hasn't provided a way to parse config");
    }

    static IStreamAwareRepProtocol initProtocol(Map<String, Object> fields,
                                                Map<String, Class<? extends IServerProtocol>> availableLogUnitProtocols,
                                                Long epoch) {
        throw new UnsupportedOperationException("This replication protocol hasn't provided a way to parse config");
    }

    default List<IServerProtocol> getLoggingUnits() {
        throw new UnsupportedOperationException("This replication protocol hasn't provided getLoggingUnits");
    }

    default List<List<IServerProtocol>> getGroups() {
        throw new UnsupportedOperationException("This replication protocol hasn't provided getGroups");
    }

    default void write(CorfuDBRuntime client, long physAddress, Map<UUID, Long> streams, byte[] data)
            throws OverwriteException, TrimmedException, OutOfSpaceException, SubLogException {
        throw new UnsupportedOperationException("This replication protocol write hasn't been implemented");
    }

    default byte[] read(CorfuDBRuntime client, long physAddress, UUID stream, long logAddress)
            throws UnwrittenException, TrimmedException {
        throw new UnsupportedOperationException("This replication protocol read hasn't been implemented");
    }
}
