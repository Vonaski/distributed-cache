package com.iksanov.distributedcache.node.config;

/**
 * Configuration holder for NetServer.
 * Defines all tunable parameters for networking.
 */
public record NetServerConfig(String host, int port, int bossThreads, int workerThreads, int backlog,
                              int maxFrameLength, int shutdownQuietPeriodSeconds, int shutdownTimeoutSeconds) {
    public static NetServerConfig defaults() {
        return new NetServerConfig("0.0.0.0", 7000, 1, 2, 256, 10 * 1024 * 1024, 2, 10);
    }
}
