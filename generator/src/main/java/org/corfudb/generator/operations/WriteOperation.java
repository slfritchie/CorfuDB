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
    public void execute() {
        String stream = (String) state.getStreams().sample(1).get(0);
        String key = (String) state.getKeys().sample(1).get(0);
        String fullKey = String.format("%s%s", stream, key);
        String value = String.format("v%d", UUID.randomUUID().getLeastSignificantBits() & 255);

        appendInvokeDescription(String.format("[:write %s %s]", fullKey, value));
        state.getMap(stream).put(key, value);
    }
}
