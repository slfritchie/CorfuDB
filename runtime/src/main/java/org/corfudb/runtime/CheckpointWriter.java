package org.corfudb.runtime;

import com.google.common.collect.ImmutableMap;
import lombok.Getter;
import org.corfudb.protocols.logprotocol.CheckpointEntry;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.runtime.collections.SMRMap;
import org.corfudb.runtime.object.ICorfuSMR;
import org.corfudb.runtime.object.ICorfuSMRProxy;
import org.corfudb.runtime.object.transactions.AbstractTransactionalContext;
import org.corfudb.runtime.object.transactions.TransactionalContext;
import org.corfudb.runtime.view.stream.BackpointerStreamView;
import org.corfudb.util.serializer.Serializers;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/** Log history checkpoint writer for SMRMaps, to be generalized to
 *  all SMR objects.
 */
public class CheckpointWriter {
    private CorfuRuntime rt;
    private UUID streamID;
    private String author;
    private SMRMap map;
    @Getter
    private UUID checkpointID;
    BackpointerStreamView sv;
    Map<String, String> mdKV = new HashMap<>();
    private LocalDateTime startTime;
    private long startAddress, endAddress;
    private long numEntries = 0, numBytes = 0;

    public CheckpointWriter(CorfuRuntime rt, UUID streamID, String author, SMRMap map) {
        this.rt = rt;
        this.streamID = streamID;
        this.author = author;
        this.map = map;
        checkpointID = UUID.randomUUID();
        sv = new BackpointerStreamView(rt, streamID);
    }

    public long startCheckpoint() {
        rt.getObjectsView().TXBegin();
        startTime = LocalDateTime.now();
        AbstractTransactionalContext context = TransactionalContext.getCurrentContext();
        long txBeginGlobalAddress = context.getSnapshotTimestamp();

        this.mdKV.put(CheckpointEntry.START_TIME, startTime.toString());
        this.mdKV.put(CheckpointEntry.START_LOG_ADDRESS, Long.toString(txBeginGlobalAddress));

        ImmutableMap<String,String> mdKV = ImmutableMap.copyOf(this.mdKV);
        CheckpointEntry cp = new CheckpointEntry(CheckpointEntry.CheckpointEntryType.START,
                author, checkpointID, mdKV, null);
        startAddress = sv.append(cp, null, null);
        System.err.printf("DBG: START's address %d, txBeginGlobalAddress %d\n", startAddress, txBeginGlobalAddress);
        return startAddress;
    }

    public void writeObjectState() {
        ImmutableMap<String,String> mdKV = ImmutableMap.copyOf(this.mdKV);
        map.keySet().stream().forEach(k -> {
            SMREntry entries[] = new SMREntry[1]; // Alloc new array each time to avoid reuse evil
            // entries[0] = new SMREntry("put", (Object[]) new Object[]{ k, map.get(k) }, Serializers.JSON);
            entries[0] = new SMREntry("put", (Object[]) new Object[]{k, ((Long) map.get(k)) + 77 }, Serializers.JSON);
            ///// entries[0] = new SMREntry("put", (Object[]) new Object[]{k, ((Integer) map.get(k)) + 77 }, Serializers.JSON);
            CheckpointEntry cp = new CheckpointEntry(CheckpointEntry.CheckpointEntryType.CONTINUATION,
                    author, checkpointID, mdKV, entries);
            long pos = sv.append(cp, null, null);
            numEntries++;
            // TODO: numBytes += mumbleMumble;
            System.err.printf("Appended CP at %d (%s %d)\n", pos, k, map.get(k));
        });
        rt.getObjectsView().TXAbort();
    }

    public long finishCheckpoint() {
        LocalDateTime endTime = LocalDateTime.now();
        mdKV.put(CheckpointEntry.END_TIME, endTime.toString());
        numEntries++;
        numBytes++;
        mdKV.put(CheckpointEntry.ENTRY_COUNT, Long.toString(numEntries));
        mdKV.put(CheckpointEntry.BYTE_COUNT, Long.toString(numBytes));

        try {
            CheckpointEntry cp = new CheckpointEntry(CheckpointEntry.CheckpointEntryType.END,
                    author, checkpointID, mdKV, null);
            endAddress = sv.append(cp, null, null);
            System.err.printf("DBG: END's address %d\n", endAddress);
            return endAddress;
        } finally {
            try {
                rt.getObjectsView().TXEnd();
            } catch (Exception e) {
                // nothing to do
            }
        }
    }
}
