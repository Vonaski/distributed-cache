package com.iksanov.distributedcache.node.config;

public record RaftConfig(
        int electionTimeoutMinMs,
        int electionTimeoutMaxMs,
        int heartbeatIntervalMs,
        int rpcTimeoutMs
) {
    public static RaftConfig defaults() {
        return new RaftConfig(150, 300, 50, 100);
    }
}