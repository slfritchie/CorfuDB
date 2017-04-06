package org.corfudb.protocols.logprotocol;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import lombok.*;
import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.Address;
import org.corfudb.util.serializer.Serializers;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Object & serialization methods for in-stream checkpoint
 * summarization of SMR object state.
 */
@ToString(callSuper = true)
@NoArgsConstructor
public class CheckpointEntry extends LogEntry {

    @RequiredArgsConstructor
    public enum CheckpointEntryType {
        START(1),           // Mandatory: 1st record in checkpoint
        CONTINUATION(2),    // Optional: 2nd through (n-1)th record
        END(3);             // Mandatory: final record checkpoint

        public final int type;

        public byte asByte() {
            return (byte) type;
        }
    };

    /**
     * Constants for use as keys in our 'dict' map.
     */
    public static String START_TIME = "Start time";
    public static String END_TIME = "End time";
    public static String START_LOG_ADDRESS = "Start log address";
    public static String ENTRY_COUNT = "Entry count";
    public static String BYTE_COUNT = "Byte count";

    /** Type of entry
     */
    @Getter
    CheckpointEntryType cpType;

    /**
     * Unique identifier for this checkpoint.  All entries
     * for the same checkpoint state must use the same ID.
     */
    @Getter
    UUID checkpointID;

    /** Author/cause/trigger of this checkpoint
     */
    @Getter
    String checkpointAuthorID;

    /** Map of checkpoint metadata, see key constants above
     */
    @Getter
    Map<String, String> dict;

    /** Optional: array of SMREntry objects that contains
     *  SMR object state of the stream that we're checkpointing.
     *  May be present in any CheckpointEntryType, but typically
     *  used by CONTINUATION entries.
     */
    @Getter
    SMREntry[] smrEntries = null;

    public CheckpointEntry(CheckpointEntryType type, String authorID, UUID checkpointID,
                           Map<String,String> dict, SMREntry[] smrEntries) {
        super(LogEntryType.CHECKPOINT);
        this.cpType = type;
        this.checkpointID = checkpointID;
        this.checkpointAuthorID = authorID;
        this.dict = dict;
        this.smrEntries = smrEntries;
    }

    static final Map<Byte, CheckpointEntryType> typeMap =
            Arrays.stream(CheckpointEntryType.values())
                    .collect(Collectors.toMap(CheckpointEntryType::asByte, Function.identity()));

    /**
     * This function provides the remaining buffer. Child entries
     * should initialize their contents based on the buffer.
     *
     * @param b The remaining buffer.
     * @param rt The CorfuRuntime used by the SMR object.
     * @return A CheckpointEntry.
     */
    @Override
    void deserializeBuffer(ByteBuf b, CorfuRuntime rt) {
        super.deserializeBuffer(b, rt);
        cpType = typeMap.get(b.readByte());
        long cpidMSB = b.readLong();
        long cpidLSB = b.readLong();
        checkpointID = new UUID(cpidMSB, cpidLSB);
        checkpointAuthorID = deserializeString(b);
        dict = new HashMap<>();
        short mapEntries = b.readShort();
        for (short i = 0; i < mapEntries; i++) {
            String k = deserializeString(b);
            String v = deserializeString(b);
            dict.put(k, v);
        }
        smrEntries = null;
        int items = b.readShort();
        if (items > 0) {
            smrEntries = new SMREntry[items];
            for (int i = 0; i < items; i++) {
                int len = b.readInt();
                ByteBuf rBuf = PooledByteBufAllocator.DEFAULT.buffer();
                b.readBytes(rBuf, len);
                SMREntry e = (SMREntry) SMREntry.deserialize(rBuf, runtime);
                // VersionLockedObject::syncStreamUnsafe checks this entry's
                // global log address for upcall management.  Checkpoint data
                // doesn't leave any trace in those upcall results, but we
                // need a stub of LogData to avoid crashing upcall management.
                LogData l = new LogData(DataType.CHECKPOINT);
                e.setEntry(l);
                smrEntries[i] = e;
            }
        }
    }

    /**
     * Serialize the given LogEntry into a given byte buffer.
     *
     * @param b The buffer to serialize into.
     */
    @Override
    public void serialize(ByteBuf b) {
        super.serialize(b);
        b.writeByte(cpType.asByte());
        b.writeLong(checkpointID.getMostSignificantBits());
        b.writeLong(checkpointID.getLeastSignificantBits());
        serializeString(checkpointAuthorID, b);
        b.writeShort(dict == null ? 0 : dict.size());
        if (dict != null) {
            dict.entrySet().stream()
                    .forEach(x -> {
                        serializeString(x.getKey(), b);
                        serializeString(x.getValue(), b);
                    });
        }
        if (smrEntries != null) {
            b.writeShort(smrEntries.length);
            for (int i = 0; i < smrEntries.length; i++) {
                ByteBuf smrEntryABuf = PooledByteBufAllocator.DEFAULT.buffer();
                smrEntries[i].serialize(smrEntryABuf);
                b.writeInt(smrEntryABuf.readableBytes());
                b.writeBytes(smrEntryABuf);
            }
        } else {
            b.writeShort(0);
        }
    }

    /** Helper function to deserialize a String.
     *
     * @param b
     * @return A String.
     */
    private String deserializeString(ByteBuf b) {
        short len = b.readShort();
        byte bytes[] = new byte[len];
        b.readBytes(bytes, 0, len);
        return new String(bytes);
    }

    /** Helper function to serialize a String.
     *
     * @param b
     */
    private void serializeString(String s, ByteBuf b) {
        b.writeShort(s.length());
        b.writeBytes(s.getBytes());
    }

    /**
     * Hex dump readable contents of ByteBuf to stdout.
     *
     * @param b ByteBuf with readable bytes available.
     */
    public static void dump(ByteBuf b) {
        byte[] bulk = new byte[b.readableBytes()];
        int oldReaderIndex = b.readerIndex();
        b.readBytes(bulk, 0, b.readableBytes() - 1);
        b.readerIndex(oldReaderIndex);
        dump(bulk);
    }

    /**
     * Hex dump contents of byte[] to stdout.
     *
     * @param bulk Bytes.
     */
    public static void dump(byte[] bulk) {
        if (bulk != null) {
            System.out.printf("Bulk(%d): ", bulk.length);
            for (int i = 0; i < bulk.length; i++) {
                System.out.printf("%x,", bulk[i]);
            }
            System.out.printf("\n");
        }
    }
}
