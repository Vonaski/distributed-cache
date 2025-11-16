package com.iksanov.distributedcache.node.config;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Application configuration for a single CacheNode.
 * Loaded purely from environment variables (12-factor style).
 */
public record ApplicationConfig(
        String nodeId,
        String host,
        int port,
        int raftPort,
        int cacheMaxSize,
        long cacheTtlMillis,
        List<String> clusterNodes,
        int metricsPort,
        boolean productionMode
) {

    public static ApplicationConfig fromEnv() {
        String nodeId = getEnv("CACHE_NODE_ID", "node-" + UUID.randomUUID().toString().substring(0, 8));
        String host = getEnv("CACHE_NODE_HOST", "0.0.0.0");
        int port = getEnvInt("CACHE_NODE_PORT", 7000);
        int raftPort = getEnvInt("CACHE_NODE_RAFT_PORT", 7100);
        int cacheMaxSize = getEnvInt("CACHE_MAX_SIZE", 10000);
        long cacheTtlMillis = getEnvLong("CACHE_TTL_MILLIS", 0L);
        int metricsPort = getEnvInt("METRICS_PORT", 8081);
        boolean productionMode = getEnvBool("CACHE_PRODUCTION_MODE", false);
        List<String> clusterNodes = parseClusterNodes(getEnv("CACHE_CLUSTER_NODES", ""));

        return new ApplicationConfig(nodeId, host, port, raftPort, cacheMaxSize, cacheTtlMillis, clusterNodes, metricsPort, productionMode);
    }

    public static ApplicationConfig defaults() {
        return new ApplicationConfig("node-1", "0.0.0.0", 7000, 7100, 10000, 0, List.of(), 8081, false);
    }

    private static String getEnv(String key, String defaultValue) {
        String v = System.getenv(key);
        return v != null && !v.isBlank() ? v : defaultValue;
    }

    private static int getEnvInt(String key, int defaultValue) {
        String v = System.getenv(key);
        if (v == null || v.isBlank()) return defaultValue;
        try { return Integer.parseInt(v); } catch (NumberFormatException e) { return defaultValue; }
    }

    private static long getEnvLong(String key, long defaultValue) {
        String v = System.getenv(key);
        if (v == null || v.isBlank()) return defaultValue;
        try { return Long.parseLong(v); } catch (NumberFormatException e) { return defaultValue; }
    }

    private static boolean getEnvBool(String key, boolean defaultValue) {
        String v = System.getenv(key);
        return v != null ? Boolean.parseBoolean(v) : defaultValue;
    }

    private static List<String> parseClusterNodes(String nodesStr) {
        if (nodesStr == null || nodesStr.isBlank()) return List.of();
        return Arrays.stream(nodesStr.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.toList());
    }

    @Override
    public String toString() {
        return "ApplicationConfig[" +
                "nodeId=" + nodeId +
                ", host=" + host +
                ", port=" + port +
                ", raftPort=" + raftPort +
                ", cacheMaxSize=" + cacheMaxSize +
                ", cacheTtlMillis=" + cacheTtlMillis +
                ", metricsPort=" + metricsPort +
                ", productionMode=" + productionMode +
                ", clusterNodes=" + clusterNodes + "]";
    }
}