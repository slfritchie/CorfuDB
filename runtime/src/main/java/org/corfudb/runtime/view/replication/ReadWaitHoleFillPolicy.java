package org.corfudb.runtime.view.replication;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.runtime.exceptions.HoleFillRequiredException;

import javax.annotation.Nonnull;
import java.util.function.Function;

import static org.corfudb.util.CoopScheduler.sched;

/** A hole filling policy which reads several times,
 * waiting a static amount of time in between, before
 * requiring a hole fill.
 *
 * Created by mwei on 4/6/17.
 */
@Slf4j
public class ReadWaitHoleFillPolicy implements IHoleFillPolicy {

    /** The amount of time to wait between reads, in milliseconds. */
    final int waitMs;

    /** The amount of times to retry before requiring a hole fill. */
    final int numRetries;

    /** Create a ReadWaitHoleFillPolicy with the given wait times
     * and retries.
     * @param waitMs        The amount of time to wait before retrying.
     * @param numRetries    The number of retries to apply before requiring a
     *                      hole fill.
     */
    static boolean warned = false;
    static int foo = 0;
    public ReadWaitHoleFillPolicy(int waitMs, int numRetries) {
        this.waitMs = waitMs;
        // this.numRetries = numRetries;
        this.numRetries = 1;
        if (!warned) { System.err.printf("\nNOTE: Hardcoded ReadWaitHoleFillPolicy.numRetries = %d\n", this.numRetries); warned = true;}
    }

    /** {@inheritDoc} */
    @Nonnull
    @Override
    public ILogData peekUntilHoleFillRequired(long address,
           Function<Long, ILogData> peekFunction) throws HoleFillRequiredException {
        int tryNum = 0;
        do {
            // If this is not the first try, sleep before trying again
            if (tryNum != 0) {
                try {
                    log.trace("peekUntilHoleFillRequired: sleep {} msec iteration {} of {}", waitMs, tryNum, numRetries);
                    // TODO: Add deterministic control over exiting this loop early?
                    sched();
                    Thread.sleep(waitMs);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
            // Try the read
            ILogData data = peekFunction.apply(address);
            // If it was not null, we can return it.
            if (data != null) {
                return data;
            }
            // Otherwise increment the counter and try again.
            tryNum++;
        } while (numRetries > tryNum);

        throw new HoleFillRequiredException("No data after " + tryNum + " retries");
    }
}
