package org.corfudb.generator.operations;

import java.util.UUID;

import lombok.extern.slf4j.Slf4j;

/**
 * Created by maithem on 7/14/17.
 */
@Slf4j
public class WriteOperation extends Operation {

    public WriteOperation() {
    }

    @Override
    public void execute(BaseOperation base) {
        String stream = (String) base.state.getStreams().sample(1).get(0);
        String key = (String) base.state.getKeys().sample(1).get(0);
        String fullKey = String.format("%s%s", stream, key);
        String value = String.format("%d", UUID.randomUUID().getLeastSignificantBits() & 255);

        base.appendInvokeDescription(String.format("[:write :%s %s]", fullKey, value));
        base.state.getMap(stream).put(key, value);
        base.appendResultDescription(String.format("[:write :%s %s]", fullKey, value));
    }
}
