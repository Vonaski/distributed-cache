package com.iksanov.distributedcache.node.config;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Cluster configuration: list of shards (each a Raft group).
 * Typically loaded from environment variable or JSON/YAML config file.
 */
public record ClusterConfig(List<ShardConfig> shards) {
    private static final Logger log = LoggerFactory.getLogger(ClusterConfig.class);

    /**
     * Loads cluster configuration from environment variable CACHE_CLUSTER_CONFIG.
     * <p>
     * IMPORTANT: Classic Sharding Architecture
     * Each physical node must participate in EXACTLY ONE shard.
     * This prevents cache isolation issues and ensures data consistency.
     * <p>
     * Expected JSON format (3 shards, 9 total nodes - CORRECT):
     * <pre>
     * [
     *   {
     *     "shardId": "shard-0",
     *     "raftPort": 7100,
     *     "peers": [
     *       "node-1:192.168.1.10:7100",
     *       "node-2:192.168.1.11:7100",
     *       "node-3:192.168.1.12:7100"
     *     ]
     *   },
     *   {
     *     "shardId": "shard-1",
     *     "raftPort": 7100,
     *     "peers": [
     *       "node-4:192.168.1.13:7100",
     *       "node-5:192.168.1.14:7100",
     *       "node-6:192.168.1.15:7100"
     *     ]
     *   },
     *   {
     *     "shardId": "shard-2",
     *     "raftPort": 7100,
     *     "peers": [
     *       "node-7:192.168.1.16:7100",
     *       "node-8:192.168.1.17:7100",
     *       "node-9:192.168.1.18:7100"
     *     ]
     *   }
     * ]
     * </pre>
     * <p>
     * For local testing with 3 nodes on same machine (single shard):
     * <pre>
     * [
     *   {
     *     "shardId": "shard-0",
     *     "raftPort": 7100,
     *     "peers": [
     *       "node-1:localhost:7100",
     *       "node-2:localhost:7101",
     *       "node-3:localhost:7102"
     *     ]
     *   }
     * ]
     * </pre>
     *
     * @return cluster configuration
     */
    public static ClusterConfig fromEnv() {
        String json = System.getenv("CACHE_CLUSTER_CONFIG");
        if (json == null || json.isBlank()) {
            log.warn("╔══════════════════════════════════════════════════════════════════╗");
            log.warn("║ CACHE_CLUSTER_CONFIG not set - starting without shards          ║");
            log.warn("║                                                                  ║");
            log.warn("║ To configure a Raft cluster, set CACHE_CLUSTER_CONFIG env var:  ║");
            log.warn("║                                                                  ║");
            log.warn("║ Example (3-node cluster):                                        ║");
            log.warn("║ [{                                                               ║");
            log.warn("║   \"shardId\": \"shard-0\",                                          ║");
            log.warn("║   \"raftPort\": 7100,                                              ║");
            log.warn("║   \"peers\": [                                                     ║");
            log.warn("║     \"node-1:192.168.1.10:7100\",                                  ║");
            log.warn("║     \"node-2:192.168.1.11:7100\",                                  ║");
            log.warn("║     \"node-3:192.168.1.12:7100\"                                   ║");
            log.warn("║   ]                                                              ║");
            log.warn("║ }]                                                               ║");
            log.warn("║                                                                  ║");
            log.warn("║ Note: Single-node Raft is not supported. Use at least 2 peers.  ║");
            log.warn("╚══════════════════════════════════════════════════════════════════╝");
            return new ClusterConfig(List.of());
        }

        try {
            ObjectMapper mapper = new ObjectMapper();
            List<ShardConfig> list = mapper.readValue(json, new TypeReference<>() {});
            log.info("Successfully loaded cluster config with {} shard(s)", list.size());
            return new ClusterConfig(list);
        } catch (Exception e) {
            log.error("Failed to parse CACHE_CLUSTER_CONFIG JSON", e);
            log.error("Expected format: [{\"shardId\":\"shard-0\",\"raftPort\":7100,\"peers\":[\"node-1:host:7100\",\"node-2:host:7100\"]}]");
            throw new IllegalArgumentException("Invalid CACHE_CLUSTER_CONFIG format", e);
        }
    }
}
