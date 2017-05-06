package org.corfudb.runtime;

import lombok.Getter;
import org.corfudb.protocols.logprotocol.CheckpointEntry;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.runtime.collections.SMRMap;
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
        startTime = LocalDateTime.now();
        mdKV.put("Start time", startTime.toString());
        CheckpointEntry cp = new CheckpointEntry(CheckpointEntry.CheckpointEntryType.START,
                author, checkpointID, mdKV, null);
        startAddress = sv.append(cp, null, null);
        System.err.printf("DBG: startAddress was %d\n", endAddress);
        return startAddress;
    }

    public void writeObjectState() {
        // TODO: We need to sync/reconcile/wrestle the log offset at TXBegin
        // and the log address assigned by startCheckpoint().
        rt.getObjectsView().TXBegin();
        SMREntry entries[] = new SMREntry[1];

        map.keySet().stream().forEach(k -> {
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
        startTime = LocalDateTime.now();
        mdKV.put("End time", startTime.toString());
        numEntries++;
        // TODO: numBytes += mumbleMumble

        mdKV.put("Entry count", Long.toString(numEntries));
        mdKV.put("Byte count", Long.toString(numBytes));
        mdKV.put("Start address", Long.toString(startAddress));

        CheckpointEntry cp = new CheckpointEntry(CheckpointEntry.CheckpointEntryType.END,
                author, checkpointID, mdKV, null);
        endAddress = sv.append(cp, null, null);
        System.err.printf("DBG: endAddress was %d\n", endAddress);
        return endAddress;
    }
}
