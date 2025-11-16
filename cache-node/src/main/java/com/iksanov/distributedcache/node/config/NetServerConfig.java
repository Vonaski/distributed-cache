package com.iksanov.distributedcache.node.config;

/**
 * Configuration holder for NetServer and RequestProcessor.
 * Defines all tunable parameters for networking and request processing.
 */
public record NetServerConfig(
        String host,
        int port,
        int bossThreads,
        int workerThreads,
        int backlog,
        int maxFrameLength,
        int shutdownQuietPeriodSeconds,
        int shutdownTimeoutSeconds,
        long slowRequestThresholdMs,
        long consensusTimeoutSeconds
) {
    public static NetServerConfig defaults() {
        return new NetServerConfig(
                "0.0.0.0",
                7000,
                1,
                1,
                128,
                10 * 1024 * 1024,
                2,
                10,
                100L,  // slowRequestThresholdMs
                10L    // consensusTimeoutSeconds
        );
    }
}
