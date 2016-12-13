package org.corfudb.protocols.wireprotocol;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.lang.management.ManagementFactory;
import java.util.Map;

/**
 * Created by mwei on 7/27/16.
 */
public class VersionInfo {
    @Getter
    Map<String, Object> optionsMap;

    /**
     * Statistics for LogUnitServer
     */
    @Getter
    Map<String, Object> statsLogUnitServer;

    /**
     * Statistics for LogUnitServer cache
     */
    @Getter
    Map<String, Object> statsLogUnitServerCache;

    /**
     * Statistics for SequencerServer
     */
    @Getter
    Map<String, Object> statsSequencerServer;

    /**
     * Statistics for LayoutServer
     */
    @Getter
    Map<String, Object> statsLayoutServer;

    @Getter
    long upTime = ManagementFactory.getRuntimeMXBean().getUptime();

    @Getter
    String startupArgs = System.getProperty("sun.java.command");

    @Getter
    String jvmUsed = System.getProperty("java.home") + "/bin/java";

    public VersionInfo(Map<String,Object> optionsMap,
                       Map<String,Object> statsLogMap,
                       Map<String,Object> statsSeqMap,
                       Map<String,Object> statsLayoutMap,
                       Map<String,Object> statsLogCacheMap) {
        this.optionsMap = optionsMap;
        this.statsLogUnitServer = statsLogMap;
        this.statsSequencerServer = statsSeqMap;
        this.statsLayoutServer = statsLayoutMap;
        this.statsLogUnitServerCache = statsLogCacheMap;
    }
}
