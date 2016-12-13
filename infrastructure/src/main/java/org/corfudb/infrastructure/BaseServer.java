package org.corfudb.infrastructure;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import io.netty.channel.ChannelHandlerContext;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.bidimap.TreeBidiMap;
import org.corfudb.protocols.wireprotocol.*;
import org.corfudb.util.Utils;

import java.lang.invoke.MethodHandles;
import java.lang.reflect.Array;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Created by mwei on 12/8/15.
 */
@Slf4j
public class BaseServer extends AbstractServer {

    /** Options map, if available */
    @Getter
    @Setter
    public Map<String, Object> optionsMap = new HashMap<>();

    /** Handler for the base server */
    @Getter
    private final CorfuMsgHandler handler = new CorfuMsgHandler()
            .generateHandlers(MethodHandles.lookup(), this);

    /**
     * Metrics: meter (counter), histogram
     */
    public static final MetricRegistry metricsLog = new MetricRegistry();
    public static final Timer timerLogWrite = metricsLog.timer("write");
    public static final Timer timerLogCommit = metricsLog.timer("commit");
    public static final Timer timerLogRead = metricsLog.timer("read");
    public static final Timer timerLogGcInterval = metricsLog.timer("gc-interval");
    public static final Timer timerLogForceGc = metricsLog.timer("force-gc");
    public static final Timer timerLogFillHole = metricsLog.timer("fill-hole");
    public static final Timer timerLogTrim = metricsLog.timer("trim");
    public static final MetricRegistry metricsSeq = new MetricRegistry();
    public static final Timer timerSeqReq = metricsSeq.timer("token-req");
    public static final Counter counterTokenSum = metricsSeq.counter("token-sum");
    public static final Counter counterToken0 = metricsSeq.counter("token-0");
    public static final MetricRegistry metricsLayout = new MetricRegistry();
    public static final Timer timerLayoutReq = metricsLayout.timer("request");
    public static final Timer timerLayoutBootstrap = metricsLayout.timer("bootstrap");
    public static final Timer timerLayoutSetEpoch = metricsLayout.timer("set-epoch");
    public static final Timer timerLayoutPrepare = metricsLayout.timer("prepare");
    public static final Timer timerLayoutPropose = metricsLayout.timer("propose");
    public static final Timer timerLayoutCommitted = metricsLayout.timer("committed");


    /** Respond to a ping message.
     *
     * @param msg   The incoming message
     * @param ctx   The channel context
     * @param r     The server router.
     */
    @ServerHandler(type=CorfuMsgType.PING)
    private static void ping(CorfuMsg msg, ChannelHandlerContext ctx, IServerRouter r) {
        r.sendResponse(ctx, msg, CorfuMsgType.PONG.msg());
    }

    /** Respond to a version request message.
     *
     * @param msg   The incoming message
     * @param ctx   The channel context
     * @param r     The server router.
     */
    @ServerHandler(type=CorfuMsgType.VERSION_REQUEST)
    private void getVersion(CorfuMsg msg, ChannelHandlerContext ctx, IServerRouter r) {
        Map<String,Object> logStats = new HashMap<>();
        dumpTimers(metricsLog, logStats);
        Map<String,Object> seqStats = new HashMap<>();
        dumpTimers(metricsSeq, seqStats);
        seqStats.put("tokens-sum", counterTokenSum);
        seqStats.put("tokens-0", counterToken0);
        Map<String,Object> layoutStats = new HashMap<>();
        dumpTimers(metricsLayout, layoutStats);

        Map<String,Object> logCacheStats = new TreeMap<>();
        try {
            CacheStats logCS = CorfuServer.getLogUnitServer().getDataCache().stats();
            dumpCaffieneStats(logCS, logCacheStats);
            logCacheStats.put("estimated-size", CorfuServer.getLogUnitServer().getDataCache().estimatedSize());
        } catch (NullPointerException e) {
            // Don't add LogUnit cache stats: there's no such component configured (e.g. during unit test)
        }

        VersionInfo vi = new VersionInfo(optionsMap, logStats, seqStats, layoutStats, logCacheStats);
        r.sendResponse(ctx, msg, new JSONPayloadMsg<>(vi, CorfuMsgType.VERSION_RESPONSE));
    }

    /** Reset the JVM. This mechanism leverages that corfu_server runs in a bash script
     * which monitors the exit code of Corfu. If the exit code is 100, then it restarts
     * the server.
     *
     * @param msg   The incoming message
     * @param ctx   The channel context
     * @param r     The server router.
     */
    @ServerHandler(type=CorfuMsgType.RESET)
    private static void doReset(CorfuMsg msg, ChannelHandlerContext ctx, IServerRouter r) {
        log.warn("Remote reset requested from client " + msg.getClientID());
        r.sendResponse(ctx, msg, CorfuMsgType.ACK.msg());
        Utils.sleepUninterruptibly(500); // Sleep, to make sure that all channels are flushed...
        System.exit(100);
    }

    @Override
    public void reset() { }

    @Override
    public void reboot() { }

    private static double convertMsec(double ns) {
        return ns / 1_000_000;
    }

    private void dumpTimers(MetricRegistry srcReg, Map<String, Object> dstStats) {
        srcReg.getTimers().forEach((k, t) -> {
            TreeMap<String, Object> m = new TreeMap<>();
            m.put("count", t.getCount());
            m.put("mean rate", t.getMeanRate());
            m.put("1-minute rate", t.getOneMinuteRate());
            m.put("5-minute rate", t.getFiveMinuteRate());
            m.put("15-minute rate", t.getFifteenMinuteRate());
            final Snapshot snapshot = t.getSnapshot();
            m.put("min", convertMsec(snapshot.getMin()));
            m.put("max", convertMsec(snapshot.getMax()));
            m.put("mean", convertMsec(snapshot.getMean()));
            m.put("50%", convertMsec(snapshot.getMedian()));
            m.put("75%", convertMsec(snapshot.get75thPercentile()));
            m.put("95%", convertMsec(snapshot.get95thPercentile()));
            m.put("99%", convertMsec(snapshot.get99thPercentile()));
            m.put("99.9%", convertMsec(snapshot.get999thPercentile()));
            dstStats.put(k, m);
        });
    }

    private void dumpCaffieneStats(CacheStats src, Map<String,Object> dst) {
        if (src == null) { return; }
        dst.put("evictions", src.evictionCount());
        dst.put("hit-rate", src.hitRate());
        dst.put("hits", src.hitCount());
        dst.put("misses", src.missCount());
    }
}
