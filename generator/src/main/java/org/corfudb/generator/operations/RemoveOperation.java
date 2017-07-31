package org.corfudb.generator.operations;

import lombok.extern.slf4j.Slf4j;

/**
 * Created by maithem on 7/14/17.
 */
@Slf4j
public class RemoveOperation extends Operation {

    public RemoveOperation() {
    }

    @Override
    public void execute() {
        String stream = (String) state.getStreams().sample(1).get(0);
        String key = (String) state.getKeys().sample(1).get(0);
        String fullKey = String.format("%s%s", stream, key);

        appendInvokeDescription(String.format("[:write %s nil]", fullKey));
        state.getMap(stream).remove(key);
    }
}
