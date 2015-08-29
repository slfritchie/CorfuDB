package org.corfudb.runtime.protocols.replications;

import org.corfudb.infrastructure.thrift.Hints;
import org.corfudb.runtime.*;
import org.corfudb.runtime.protocols.IServerProtocol;
import org.corfudb.runtime.protocols.logunits.IWriteOnceLogUnit;
import org.corfudb.runtime.smr.MultiCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonString;
import javax.json.JsonValue;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.*;
import java.util.regex.Matcher;

/**
 * @author dmalkhi
 */
public class QuorumReplicationProtocol extends ChainReplicationProtocol implements IReplicationProtocol {
    private static final Logger log = LoggerFactory.getLogger(QuorumReplicationProtocol.class);

    public QuorumReplicationProtocol(List<List<IServerProtocol>> groups) {
        super(groups);
    }

    public static String getProtocolString()
    {
        return "cdbqr";
    }

    public static Map<String, Object> segmentParser(JsonObject jo) {
        Map<String, Object> ret = new HashMap<String, Object>();
        ArrayList<Map<String, Object>> groupList = new ArrayList<Map<String, Object>>();
        for (JsonValue j2 : jo.getJsonArray("groups"))
        {
            HashMap<String,Object> groupItem = new HashMap<String,Object>();
            JsonArray ja = (JsonArray) j2;
            ArrayList<String> group = new ArrayList<String>();
            for (JsonValue j3 : ja)
            {
                group.add(((JsonString)j3).getString());
            }
            groupItem.put("nodes", group);
            groupList.add(groupItem);
        }
        ret.put("groups", groupList);

        return ret;
    }

    @Override
    public void write(long address, Set<String> streams, byte[] data)
            throws OverwriteException, TrimmedException, OutOfSpaceException, NetworkException {
        int mod = getGroups().size();
        int groupnum =(int) (address % mod);
        List<IServerProtocol> chain = getGroups().get(groupnum);
        //writes have to go to chain in order
        long mappedAddress = address/mod;
        int ntimeouts = 0;

        for (IServerProtocol unit : chain)
        {
            try {
                ((IWriteOnceLogUnit)unit).write(mappedAddress, streams, data);
            } catch (TrimmedException e) {
                throw e;
            } catch (OverwriteException|NetworkException|OutOfSpaceException e) {
                // the current policy is to try and grab any many logging-units as we can.
                // if more than a minority rejects are encountered, we cannot commit, and we throw one of the exceptions we encountered.
                // otherwise, the write commits on a majority and returns.
                //
                // TODO logging-units should support overwriting conditioned on ballot! otherwise, committed writes on a majority may disappear due to a unit failure.
                //
                if (++ntimeouts > chain.size() / 2)
                    throw e;
                else
                    continue;
            }
        }
        return;

    }

    @Override
    public byte[] read(long address, String stream) throws UnwrittenException, TrimmedException, NetworkException {
        int mod = getGroups().size();
        int groupnum =(int) (address % mod);
        long mappedAddress = address/mod;

        List<IServerProtocol> chain = getGroups().get(groupnum);
        byte[] ret = null;
        int nsucceeded = 0;

        for (IServerProtocol unit : chain) {
            // the current policy is to try to read logging-units until a commit-majority is accumulated.
            //
            try {
                ret = ((IWriteOnceLogUnit) unit).read(mappedAddress, stream); // TODO verify all read values are identical!
                nsucceeded++;
            } catch (TrimmedException e) {
                throw e;
            } catch (UnwrittenException | NetworkException e) {
            }
        }

        if (nsucceeded > chain.size()/2)
            return ret;
        else
            throw new UnwrittenException("no quorum commit found", address);
    }

}
