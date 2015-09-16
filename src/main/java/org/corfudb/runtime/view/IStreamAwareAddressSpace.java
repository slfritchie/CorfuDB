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

package org.corfudb.runtime.view;

import org.corfudb.runtime.*;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import java.util.UUID;

/**
 * This interface represents a view on write-once address spaces that are aware of streams. This address space can only
 * work with other interfaces of the form IStreamAware* The point is that writes and reads should all give a global
 * (physical) address and a local (logical) stream address.
 *
 * Created by taia on 9/15/15
 */

public interface IStreamAwareAddressSpace {
    void write(long address, Map<UUID, Long> streams, Serializable s)
            throws IOException, OverwriteException, TrimmedException, OutOfSpaceException, SubLogException;

    void write(long address, Map<UUID, Long> streams, byte[] data) throws OverwriteException, TrimmedException, OutOfSpaceException, SubLogException;

    byte[] read(long logAddress, UUID stream) throws UnwrittenException, TrimmedException;

    Object readObject(long logAddress, UUID stream)
            throws UnwrittenException, TrimmedException, ClassNotFoundException, IOException;

    byte[] read(long physAddress) throws UnwrittenException, TrimmedException;

    Object readObject(long address)
            throws UnwrittenException, TrimmedException, ClassNotFoundException, IOException;
}

