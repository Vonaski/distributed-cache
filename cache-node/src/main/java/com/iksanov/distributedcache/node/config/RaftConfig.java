package com.iksanov.distributedcache.node.config;

public record RaftConfig(long electionTimeoutMinMs, long electionTimeoutMaxMs, long heartbeatIntervalMs,
                         long voteWaitTimeoutMs) {
    public RaftConfig {
        if (electionTimeoutMinMs <= 0 || electionTimeoutMaxMs <= electionTimeoutMinMs)
            throw new IllegalArgumentException("Invalid election timeout bounds");
    }

    public static RaftConfig defaults() {
        return new RaftConfig(150, 300, 50, 300);
    }
}
