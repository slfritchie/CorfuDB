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
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

/** Checkpoint writer for SMRMaps: take a snapshot of the
 *  object via TXBegin(), then dump the frozen object's
 *  state into CheckpointEntry records into the object's
 *  stream.
 *
 * TODO: Generalize to all SMR objects.
 */
public class CheckpointWriter {
    /** Metadata to be stored in the CP's 'dict' map.
     */
    private UUID streamID;
    private String author;
    @Getter
    private UUID checkpointID;
    private LocalDateTime startTime;
    private long startAddress, endAddress;
    private long numEntries = 0, numBytes = 0;
    Map<String, String> mdKV = new HashMap<>();

    /** Local ref to the object's runtime.
     */
    private CorfuRuntime rt;

    /** Local ref to the stream's view.
     */
    BackpointerStreamView sv;

    /** Local ref to the object that we're dumping.
     *  TODO: generalize to all SMR objects.
     */
    private SMRMap map;

    public CheckpointWriter(CorfuRuntime rt, UUID streamID, String author, SMRMap map) {
        this.rt = rt;
        this.streamID = streamID;
        this.author = author;
        this.map = map;
        checkpointID = UUID.randomUUID();
        sv = new BackpointerStreamView(rt, streamID);
    }

    /** Append a checkpoint START record to this object's stream.
     *
     * @return Global log address of the START record.
     */
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
