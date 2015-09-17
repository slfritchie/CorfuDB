/**
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.corfudb.runtime.protocols.logunits;

import org.corfudb.infrastructure.thrift.ExtntWrap;
import org.corfudb.runtime.*;
import org.corfudb.runtime.protocols.IServerProtocol;

import java.util.Map;
import java.util.UUID;

/**
 * This interface is just like the IWriteOnceLogUnit, except with a stream-aware write, and without some hinting
 * features.
 *
 * All methods are synchronous, that is, they block until successful completion
 * of the command.
 */

public interface IStreamAwareLogUnit extends IServerProtocol {
    void streamAwareWrite(long globalAddress, Map<UUID, Long> streams, byte[] payload)
            throws OverwriteException, TrimmedException, NetworkException, OutOfSpaceException, SubLogException;
    byte[] read(long streamAddress, UUID stream) throws UnwrittenException, TrimmedException, NetworkException;
    void trim(long address) throws NetworkException;

    /**
     * Gets the highest address written to this log unit. Some units may not support this operation and
     * will throw an UnsupportedOperationException
     * @return                      The highest address written to this logunit.
     * @throws NetworkException     If the log unit could not be contacted.
     */
    default long highestAddress() throws NetworkException
    {
        throw new UnsupportedOperationException("Log unit doesn't support querying latest address!");
    }

    default void setCommit(long address, UUID stream, boolean commit) throws TrimmedException, NetworkException {
        throw new UnsupportedOperationException("Log unit doesn't support commit bits");
    }

    default ExtntWrap fullRead(long address, UUID stream) throws UnwrittenException, TrimmedException, NetworkException {
        throw new UnsupportedOperationException("Log unit doesn't support fullReads");
    }
}

