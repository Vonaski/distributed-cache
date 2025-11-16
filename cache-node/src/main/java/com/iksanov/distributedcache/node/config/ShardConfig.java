package com.iksanov.distributedcache.node.config;

import java.util.List;

/**
 * Describes a single shard (Raft group) in the cluster.
 * <p>
 * Each shard represents a separate Raft consensus group that replicates data.
 * Multiple nodes participate in each shard for fault tolerance.
 * <p>
 * <b>Peer Format:</b> Each peer string must follow the format: {@code "nodeId:host:port"}
 * <ul>
 *   <li><b>nodeId</b> - Unique identifier for the node (e.g., "node-1")</li>
 *   <li><b>host</b> - Hostname or IP address (e.g., "192.168.1.10" or "localhost")</li>
 *   <li><b>port</b> - Raft transport port (e.g., 7100)</li>
 * </ul>
 * <p>
 * <b>Example:</b>
 * <pre>
 * new ShardConfig(
 *     "shard-0",
 *     7100,
 *     List.of(
 *         "node-1:192.168.1.10:7100",
 *         "node-2:192.168.1.11:7100",
 *         "node-3:192.168.1.12:7100"
 *     )
 * )
 * </pre>
 * <p>
 * <b>Important:</b> The peers list should include ALL nodes that participate in this shard,
 * including the local node. The ShardManager will automatically filter out the local node
 * when creating the Raft peer list.
 */
public record ShardConfig(
        String shardId,
        int raftPort,
        List<String> peers
) {
    /**
     * Validates the shard configuration.
     *
     * @throws IllegalArgumentException if configuration is invalid
     */
    public ShardConfig {
        if (shardId == null || shardId.isBlank()) {
            throw new IllegalArgumentException("shardId cannot be null or blank");
        }
        if (raftPort <= 0 || raftPort > 65535) {
            throw new IllegalArgumentException("raftPort must be between 1 and 65535");
        }
        if (peers == null || peers.isEmpty()) {
            throw new IllegalArgumentException("peers list cannot be null or empty");
        }

        // Validate peer format: "nodeId:host:port"
        for (String peer : peers) {
            String[] parts = peer.split(":");
            if (parts.length != 3) {
                throw new IllegalArgumentException(
                    "Invalid peer format: '" + peer + "'. Expected format: 'nodeId:host:port'"
                );
            }
            try {
                int port = Integer.parseInt(parts[2]);
                if (port <= 0 || port > 65535) {
                    throw new IllegalArgumentException(
                        "Invalid port in peer '" + peer + "': port must be between 1 and 65535"
                    );
                }
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(
                    "Invalid port in peer '" + peer + "': port must be a number"
                );
            }
        }
    }
}
