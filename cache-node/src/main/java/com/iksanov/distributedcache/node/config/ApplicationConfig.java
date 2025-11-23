package com.iksanov.distributedcache.node.config;

import java.util.List;
import java.util.UUID;

/**
 * Application configuration record for CacheNode.
 * Simple and immutable, similar to NetServerConfig pattern.
 */
public record ApplicationConfig(
        String nodeId,
        String host,
        int port,
        int replicationPort,
        int metricsPort,
        int cacheMaxSize,
        long cacheTtlMillis,
        boolean clusterEnabled,
        List<String> clusterNodes,
        NodeRole role,
        int priority
) {
    public enum NodeRole {
        MASTER,
        REPLICA
    }

    public ApplicationConfig {
        if (priority < 0) {
            throw new IllegalArgumentException("Priority must be >= 0");
        }
    }

    public static ApplicationConfig fromEnv() {
        return new ApplicationConfig(
                getEnv("CACHE_NODE_ID", "node-" + UUID.randomUUID().toString().substring(0, 8)),
                getEnv("CACHE_NODE_HOST", "0.0.0.0"),
                getEnvInt("CACHE_NODE_PORT", 7000),
                getEnvInt("CACHE_NODE_REPLICATION_PORT", 7100),
                getEnvInt("METRICS_PORT", 9081),
                getEnvInt("CACHE_MAX_SIZE", 10000),
                getEnvLong("CACHE_TTL_MILLIS", 0),
                getEnvBool("CACHE_CLUSTER_ENABLED", false),
                parseClusterNodes(getEnv("CACHE_CLUSTER_NODES", "")),
                parseNodeRole(getEnv("CACHE_NODE_ROLE", "MASTER")),
                getEnvInt("CACHE_NODE_PRIORITY", 0)
        );
    }

    public static ApplicationConfig defaults() {
        return new ApplicationConfig(
                "node-1",
                "0.0.0.0",
                7000,
                7100,
                9081,
                10000,
                0,
                false,
                List.of(),
                NodeRole.MASTER,
                0
        );
    }

    public NetServerConfig toNetServerConfig() {
        return new NetServerConfig(host, port, 1, 1, 128, 10 * 1024 * 1024, 2, 10);
    }

    private static String getEnv(String key, String defaultValue) {
        String value = System.getenv(key);
        return value != null ? value : defaultValue;
    }

    private static int getEnvInt(String key, int defaultValue) {
        String value = System.getenv(key);
        if (value == null) return defaultValue;
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    private static long getEnvLong(String key, long defaultValue) {
        String value = System.getenv(key);
        if (value == null) return defaultValue;
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    private static boolean getEnvBool(String key, boolean defaultValue) {
        String value = System.getenv(key);
        return value != null ? Boolean.parseBoolean(value) : defaultValue;
    }

    private static List<String> parseClusterNodes(String nodesStr) {
        if (nodesStr == null || nodesStr.isBlank()) {
            return List.of();
        }
        return List.of(nodesStr.split(","));
    }

    private static NodeRole parseNodeRole(String roleStr) {
        try {
            return NodeRole.valueOf(roleStr.toUpperCase());
        } catch (IllegalArgumentException e) {
            return NodeRole.MASTER;
        }
    }
}