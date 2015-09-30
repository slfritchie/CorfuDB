package org.corfudb.infrastructure.wireprotocol;

import io.netty.buffer.ByteBuf;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;


/**
 * Created by taia on 9/29/15.
 */
@Getter
@Setter
@NoArgsConstructor
public class NettyLogUnitCommitMsg extends NettyCorfuMsg {


    long address;
    Map<UUID, Long> streams = new HashMap<UUID, Long>();
    boolean commit;

    public NettyLogUnitCommitMsg(long address, Map<UUID, Long> streams, boolean commit)
    {
        this.msgType = NettyCorfuMsgType.SET_COMMIT;
        this.address = address;
        this.streams = streams;
        this.commit = commit;
    }
    /**
     * Serialize the message into the given bytebuffer.
     *
     * @param buffer The buffer to serialize to.
     */
    @Override
    public void serialize(ByteBuf buffer) {
        super.serialize(buffer);
        buffer.writeLong(address);
        if (address == -1L) {
            buffer.writeByte(streams.size());
            for (UUID stream : streams.keySet()) {
                buffer.writeLong(stream.getMostSignificantBits());
                buffer.writeLong(stream.getLeastSignificantBits());
                buffer.writeLong(streams.get(stream));
            }
        }
        buffer.writeBoolean(commit);
    }

    /**
     * Parse the rest of the message from the buffer. Classes that extend NettyCorfuMsg
     * should parse their fields in this method.
     *
     * @param buffer
     */
    @Override
    public void fromBuffer(ByteBuf buffer) {
        super.fromBuffer(buffer);
        address = buffer.readLong();
        if (address == -1L) {
            int count = buffer.readByte();
            for (int i = 0; i < count; i++) {
                UUID stream = new UUID(buffer.readLong(), buffer.readLong());
                streams.put(stream, buffer.readLong());
            }
        }
        commit = buffer.readBoolean();

    }
}
