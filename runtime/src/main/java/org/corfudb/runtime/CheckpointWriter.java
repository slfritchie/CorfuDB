package org.corfudb.runtime;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import lombok.Getter;
import lombok.Setter;
import org.corfudb.protocols.logprotocol.CheckpointEntry;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.runtime.collections.SMRMap;
import org.corfudb.runtime.object.transactions.AbstractTransactionalContext;
import org.corfudb.runtime.object.transactions.TransactionalContext;
import org.corfudb.runtime.view.stream.BackpointerStreamView;
import org.corfudb.util.serializer.Serializers;

import java.time.LocalDateTime;
import java.util.*;
import java.util.function.Function;

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

    /** Mutator lambda to change map key.  Typically used for
     *  testing but could also be used for type conversion, etc.
     */
    @Getter
    @Setter
    Function<Object,Object> keyMutator = (x) -> x;

    /** Mutator lambda to change map value.  Typically used for
     *  testing but could also be used for type conversion, etc.
     */
    @Getter
    @Setter
    Function<Object,Object> valueMutator = (x) -> x;

    /** Batch size: number of SMREntry in a single CONTINUATION.
     */
    @Getter
    @Setter
    private int batchSize = 50;

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

    /** Static method for all steps necessary to append checkpoint
     *  data for an SMRMap into its own stream.
     */

    public static List<Long> appendCheckpoint(CorfuRuntime rt, UUID streamID, String author, SMRMap map) {
        List<Long> addrs = new ArrayList<>();
        CheckpointWriter cpw = new CheckpointWriter(rt, streamID, author, map);

        addrs.add(cpw.startCheckpoint());
        addrs.addAll(cpw.appendObjectState());
        addrs.add(cpw.finishCheckpoint());
        return addrs;
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

    /** Append zero or more CONTINUATION records to this
     *  object's stream.  Each will contain a fraction of
     *  the state of the object that we're checkpointing
     *  (up to batchSize items at a time).
     *
     *  The Iterators class appears to preserve the laziness
     *  of Stream processing; we don't wish to use more
     *  memory than strictly necessary to generate the
     *  checkpoint.  NOTE: It would be even more useful if
     *  the map had a lazy iterator: the eagerness of
     *  map.keySet().stream() is not ideal, but at least
     *  it should be much smaller than the entire map.
     *
     * @return Stream of global log addresses of the
     * CONTINUATION records written.
     */
    public List<Long> appendObjectState() {
        ImmutableMap<String,String> mdKV = ImmutableMap.copyOf(this.mdKV);
        List<Long> continuationAddresses = new ArrayList<>();

        Iterators.partition(map.keySet().stream()
            .map(k -> {
                return new SMREntry("put",
                        new Object[]{keyMutator.apply(k), valueMutator.apply(map.get(k)) },
                        Serializers.JSON);
        }).iterator(), batchSize)
            .forEachRemaining(entries -> {
                // Convert Object[] to SMREntry[], combined with byte count.
                // java.lang.ClassCastException: [Ljava.lang.Object; cannot be cast to [Lorg.corfudb.protocols.logprotocol.SMREntry;
                int numEntries = ((List) entries).size();
                SMREntry e[] = new SMREntry[numEntries];
                for (int i = 0; i < numEntries; i++) {
                    e[i] = (SMREntry) ((List) entries).get(i);
                    // TODO: get real serialization size available, somehow.
                    numBytes += 50;
                }
                CheckpointEntry cp = new CheckpointEntry(CheckpointEntry.CheckpointEntryType.CONTINUATION,
                        author, checkpointID, mdKV, e);
                long pos = sv.append(cp, null, null);
                continuationAddresses.add(pos);
                numEntries++;
            });
        rt.getObjectsView().TXAbort();
        return continuationAddresses;
    }

    public static List<CheckpointEntry> reduceFunc(List<CheckpointEntry> cps) {
        return cps;
    }

    /** Append a checkpoint END record to this object's stream.
     *
     * @return Global log address of the END record.
     */

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
