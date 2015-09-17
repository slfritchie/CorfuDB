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

package org.corfudb.runtime;
import java.io.IOException;
import java.util.UUID;

/**
 * This exception is thrown whenever a read is attempted on an unwritten page.
 */
@SuppressWarnings("serial")
public class UnwrittenException extends IOException
{
    public long physAddress;
    public UUID stream;
    public long logAddress;

    public UnwrittenException(String desc, long physAddress)
    {
        super(desc + "[address=" + physAddress + "]");
        this.physAddress = physAddress;
    }

    public UnwrittenException(String desc, UUID stream, long logAddress)
    {
        super(desc + "[address=(stream:" + stream + ", " + logAddress  + ")]");
        this.stream = stream;
        this.logAddress = logAddress;
    }
}

