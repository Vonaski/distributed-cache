package com.iksanov.distributedcache.node.config;

public record RaftConfig(
        int electionTimeoutMinMs,
        int electionTimeoutMaxMs,
        int heartbeatIntervalMs,
        int rpcTimeoutMs,
        int maxBatchSize,
        int maxBatchDelayMs
) {
    public RaftConfig {
        if (electionTimeoutMaxMs <= electionTimeoutMinMs)
            throw new IllegalArgumentException("Max election timeout must be > min");
        if (heartbeatIntervalMs >= electionTimeoutMinMs)
            throw new IllegalArgumentException("Heartbeat must be shorter than election timeout");
    }

    public static RaftConfig defaults() {
        return new RaftConfig(300, 600, 100, 200, 100, 10);
    }

    public static RaftConfig production() {
        return new RaftConfig(800, 1600, 250, 400, 200, 5);
    }

    @Override
    public String toString() {
        return String.format("RaftConfig[election=%d-%dms, heartbeat=%dms, rpcTimeout=%dms, batch=%d/%dms]",
                electionTimeoutMinMs, electionTimeoutMaxMs, heartbeatIntervalMs, rpcTimeoutMs, maxBatchSize, maxBatchDelayMs);
    }

    public static RaftConfig fromEnv() {
        int min = getEnvInt("RAFT_ELECTION_MIN_MS", 300);
        int max = getEnvInt("RAFT_ELECTION_MAX_MS", 600);
        int hb = getEnvInt("RAFT_HEARTBEAT_MS", 100);
        int rpc = getEnvInt("RAFT_RPC_TIMEOUT_MS", 200);
        int batchSize = getEnvInt("RAFT_MAX_BATCH_SIZE", 100);
        int batchDelay = getEnvInt("RAFT_MAX_BATCH_DELAY_MS", 10);
        return new RaftConfig(min, max, hb, rpc, batchSize, batchDelay);
    }

    private static int getEnvInt(String key, int def) {
        String v = System.getenv(key);
        if (v == null || v.isBlank()) return def;
        try { return Integer.parseInt(v); } catch (Exception e) { return def; }
    }
}