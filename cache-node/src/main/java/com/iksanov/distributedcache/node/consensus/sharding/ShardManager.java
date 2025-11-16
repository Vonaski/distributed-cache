package com.iksanov.distributedcache.node.consensus.sharding;

import com.iksanov.distributedcache.common.cluster.ConsistentHashRing;
import com.iksanov.distributedcache.common.cluster.NodeInfo;
import com.iksanov.distributedcache.node.config.ClusterConfig;
import com.iksanov.distributedcache.node.config.ShardConfig;
import com.iksanov.distributedcache.node.config.RaftConfig;
import com.iksanov.distributedcache.node.consensus.persistence.FileBasedRaftPersistence;
import com.iksanov.distributedcache.node.consensus.persistence.RaftPersistence;
import com.iksanov.distributedcache.node.consensus.raft.RaftStateMachine;
import com.iksanov.distributedcache.node.consensus.transport.NettyRaftTransport;
import com.iksanov.distributedcache.node.consensus.raft.DefaultRaftStateMachine;
import com.iksanov.distributedcache.node.consensus.raft.RaftNode;
import com.iksanov.distributedcache.node.core.CacheStore;
import com.iksanov.distributedcache.node.metrics.RaftMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Objects;

public final class ShardManager {
    private static final Logger log = LoggerFactory.getLogger(ShardManager.class);
    private final String nodeId;
    private final String host;
    private final CacheStore cacheStore;
    private final RaftMetrics metrics;
    private final RaftConfig raftConfig;
    private final ConsistentHashRing ring;
    private final Map<String, RaftNode> raftGroups = new ConcurrentHashMap<>();
    private final Map<String, RaftStateMachine> stateMachines = new ConcurrentHashMap<>();
    private final int virtualNodes;

    public ShardManager(String nodeId, String host, CacheStore cacheStore, RaftMetrics metrics, RaftConfig raftConfig, int virtualNodes) {
        this.nodeId = Objects.requireNonNull(nodeId, "nodeId");
        this.host = Objects.requireNonNull(host, "host");
        this.cacheStore = Objects.requireNonNull(cacheStore, "cacheStore");
        this.metrics = Objects.requireNonNull(metrics, "metrics");
        this.raftConfig = Objects.requireNonNull(raftConfig, "raftConfig");
        this.virtualNodes = Math.max(1, virtualNodes);
        this.ring = new ConsistentHashRing(this.virtualNodes);
    }

    /**
     * Validates that this node participates in at most one shard.
     * This enforces the classic sharding architecture where each physical node
     * belongs to exactly one shard, preventing cache isolation issues.
     *
     * @throws IllegalStateException if node participates in multiple shards
     */
    private void validateSingleShardParticipation(ClusterConfig clusterConfig) {
        List<String> participatingShards = new ArrayList<>();

        for (ShardConfig shard : clusterConfig.shards()) {
            for (String peerSpec : shard.peers()) {
                String[] parts = peerSpec.split(":", 3);
                if (parts.length >= 1 && parts[0].equals(nodeId)) {
                    participatingShards.add(shard.shardId());
                    break;
                }
            }
        }

        if (participatingShards.size() > 1) {
            String errorMsg = String.format(
                "╔══════════════════════════════════════════════════════════════════╗%n" +
                "║ CONFIGURATION ERROR: Node participating in multiple shards       ║%n" +
                "╠══════════════════════════════════════════════════════════════════╣%n" +
                "║ Node '%s' is configured to participate in %d shards:            ║%n" +
                "║ %s                                                               ║%n" +
                "║                                                                  ║%n" +
                "║ This configuration is NOT supported. Each physical node must    ║%n" +
                "║ participate in EXACTLY ONE shard (classic sharding).             ║%n" +
                "║                                                                  ║%n" +
                "║ Reason: All shards on the same node share one CacheStore,       ║%n" +
                "║ causing data isolation issues and incorrect eviction behavior.  ║%n" +
                "║                                                                  ║%n" +
                "║ CORRECT configuration example (3 shards, 9 total nodes):        ║%n" +
                "║   Shard-0: node-1, node-2, node-3                                ║%n" +
                "║   Shard-1: node-4, node-5, node-6                                ║%n" +
                "║   Shard-2: node-7, node-8, node-9                                ║%n" +
                "║                                                                  ║%n" +
                "║ Each node appears in exactly ONE shard.                          ║%n" +
                "╚══════════════════════════════════════════════════════════════════╝",
                nodeId, participatingShards.size(), participatingShards
            );

            log.error(errorMsg);
            throw new IllegalStateException(
                String.format("Node '%s' cannot participate in multiple shards: %s", nodeId, participatingShards)
            );
        }

        if (participatingShards.size() == 1) {
            log.info("✓ Node '{}' participates in shard '{}' (classic architecture)",
                nodeId, participatingShards.get(0));
        } else {
            log.info("Node '{}' does not participate in any shards", nodeId);
        }
    }

    public void startShards(ClusterConfig clusterConfig) {
        Objects.requireNonNull(clusterConfig, "clusterConfig");
        log.info("Starting shards for nodeId={}, host={}, virtualNodes={}", nodeId, host, virtualNodes);

        validateSingleShardParticipation(clusterConfig);

        for (ShardConfig shard : clusterConfig.shards()) {
            String shardId = shard.shardId();
            boolean localParticipates = false;
            for (String peerSpec : shard.peers()) {
                String[] parts = peerSpec.split(":", 3);
                if (parts.length >= 1 && parts[0].equals(nodeId)) {
                    localParticipates = true;
                    break;
                }
            }

            if (!localParticipates) {
                log.info("Node '{}' does not participate in shard '{}'; skipping", nodeId, shardId);
                continue;
            }

            NodeInfo ringEntry = buildRingNodeInfo(shardId, shard);
            ring.addNode(ringEntry);

            try {
                NettyRaftTransport transport = new NettyRaftTransport(nodeId, host, shard.raftPort());
                List<String> otherPeersAddresses = new ArrayList<>();
                Map<String, String> peerMappings = new HashMap<>();

                for (String peerSpec : shard.peers()) {
                    String[] parts = peerSpec.split(":", 3);
                    if (parts.length != 3) {
                        log.warn("Invalid peer format '{}' in shard '{}', skipping", peerSpec, shardId);
                        continue;
                    }

                    String peerId = parts[0];
                    String peerHost = parts[1];
                    String peerPort = parts[2];
                    String peerAddress = peerHost + ":" + peerPort;

                    // Skip self
                    if (peerId.equals(nodeId)) {
                        continue;
                    }

                    otherPeersAddresses.add(peerAddress);
                    peerMappings.put(peerId, peerAddress);
                }

                if (otherPeersAddresses.isEmpty()) {
                    log.warn("Shard '{}' has no other peers for node '{}', single-node Raft is not supported. Skipping.",
                            shardId, nodeId);
                    continue;
                }

                for (Map.Entry<String, String> entry : peerMappings.entrySet()) {
                    transport.registerPeerMapping(entry.getKey(), entry.getValue());
                    log.debug("[{}] Registered peer mapping: {} -> {}", shardId, entry.getKey(), entry.getValue());
                }

                RaftPersistence persistence = new FileBasedRaftPersistence(
                        Paths.get("./data", nodeId, shardId),
                        nodeId + "-" + shardId
                );
                RaftStateMachine stateMachine = new DefaultRaftStateMachine(cacheStore, metrics);

                RaftNode raftNode = new RaftNode(
                        nodeId + "-" + shardId,
                        otherPeersAddresses,
                        transport,
                        stateMachine,
                        persistence,
                        metrics,
                        raftConfig.electionTimeoutMinMs(),
                        raftConfig.electionTimeoutMaxMs(),
                        raftConfig.heartbeatIntervalMs(),
                        raftConfig.maxBatchSize(),
                        raftConfig.maxBatchDelayMs()
                );

                raftNode.start();
                log.info("[{}] Pre-connecting to {} peers...", shardId, otherPeersAddresses.size());
                try {
                    transport.connectToPeers(otherPeersAddresses);
                    log.info("[{}] Pre-connections established", shardId);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    log.warn("[{}] Pre-connection interrupted, will connect lazily", shardId);
                }

                raftGroups.put(shardId, raftNode);
                stateMachines.put(shardId, stateMachine);
                log.info("Started local RaftNode for shard='{}' on port={} with {} peers",
                        shardId, shard.raftPort(), otherPeersAddresses.size());

            } catch (java.io.IOException ioe) {
                log.error("I/O error while initializing persistence for shard '{}', skipping this shard: {}", shardId, ioe.getMessage(), ioe);
            } catch (Throwable t) {
                log.error("Failed to initialize RaftNode for shard '{}': {}", shardId, t.getMessage(), t);
            }
        }

        log.info("ShardManager started. local shards: {}, ring size: {}", raftGroups.keySet(), ring.ringSize());
    }

    private NodeInfo buildRingNodeInfo(String shardId, ShardConfig shard) {
        return new NodeInfo(shardId, this.host, shard.raftPort(), shard.raftPort());
    }

    public String selectShardForKey(String key) {
        if (key == null) return null;
        NodeInfo chosen = ring.getNodeForKey(key);
        if (chosen != null) return chosen.nodeId();
        List<String> local = new ArrayList<>(raftGroups.keySet());
        if (local.isEmpty()) return null;
        int idx = Math.abs(key.hashCode()) % local.size();
        return local.get(idx);
    }

    public RaftNode getShard(String shardId) {
        return raftGroups.get(shardId);
    }

    public RaftStateMachine getStateMachine(String shardId) {
        return stateMachines.get(shardId);
    }

    public Set<String> localShardIds() {
        return Collections.unmodifiableSet(raftGroups.keySet());
    }

    public void stopAll() {
        for (RaftNode node : raftGroups.values()) {
            try {
                node.stop();
            } catch (Exception e) {
                log.warn("Error stopping raft node {}", node, e);
            }
        }
        log.info("All local RaftNodes stopped");
    }
}
