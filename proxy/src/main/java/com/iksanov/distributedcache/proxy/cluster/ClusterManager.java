package com.iksanov.distributedcache.proxy.cluster;

import com.iksanov.distributedcache.common.cluster.NodeInfo;
import com.iksanov.distributedcache.common.cluster.ReplicaManager;
import com.iksanov.distributedcache.common.cluster.sharding.ConsistentHashRing;
import com.iksanov.distributedcache.proxy.config.CacheClusterConfig;
import jakarta.annotation.PostConstruct;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

@Component
public class ClusterManager {

    private static final Logger log = LoggerFactory.getLogger(ClusterManager.class);
    private final ConsistentHashRing hashRing;
    private final CacheClusterConfig config;
    @Getter
    private final ReplicaManager replicaManager;

    public ClusterManager(CacheClusterConfig config) {
        this.config = config;
        this.hashRing = new ConsistentHashRing(config.getVirtualNodes());
        this.replicaManager = new ReplicaManager();
    }

    @PostConstruct
    public void init() {
        List<String> nodeAddresses = config.getNodes();
        if (nodeAddresses.isEmpty()) throw new IllegalStateException("No cache nodes configured");
        log.info("Initializing cluster with {} nodes", nodeAddresses.size());
        Map<String, NodeInfo> mastersByNodeId = new HashMap<>();
        for (String nodeAddr : nodeAddresses) {
            try {
                NodeInfo node = NodeInfo.fromString(nodeAddr);
                if (isMasterNode(node)) {
                    hashRing.addNode(node);
                    mastersByNodeId.put(node.nodeId(), node);
                    log.info("Added MASTER node to cluster: {}", node);
                }
            } catch (Exception e) {
                log.error("Failed to parse node address '{}': {}", nodeAddr, e.getMessage());
                throw new IllegalStateException("Invalid node configuration: " + nodeAddr, e);
            }
        }

        for (String nodeAddr : nodeAddresses) {
            try {
                NodeInfo node = NodeInfo.fromString(nodeAddr);
                if (!isMasterNode(node)) {
                    String masterNodeId = extractMasterNodeId(node.nodeId());
                    NodeInfo master = mastersByNodeId.get(masterNodeId);

                    if (master == null) {
                        log.warn("Master node '{}' not found for replica '{}', skipping replica registration", masterNodeId, node.nodeId());
                        continue;
                    }
                    replicaManager.registerReplica(master, node);
                    log.info("Added REPLICA node {} for master {}", node.nodeId(), masterNodeId);
                }
            } catch (Exception e) {
                log.error("Failed to process replica node '{}': {}", nodeAddr, e.getMessage());
                throw new IllegalStateException("Invalid node configuration: " + nodeAddr, e);
            }
        }

        int totalReplicas = replicaManager.getAllMasters().stream()
                .mapToInt(masterId -> {
                    NodeInfo master = mastersByNodeId.get(masterId);
                    return master != null ? replicaManager.getReplicas(master).size() : 0;
                })
                .sum();

        log.info("Cluster initialized: {} master nodes, {} total replicas", hashRing.getNodes().size(), totalReplicas);
    }

    public NodeInfo getMasterNodeForKey(String key) {
        NodeInfo node = hashRing.getNodeForKey(key);
        if (node == null) throw new IllegalStateException("No nodes available in cluster");
        return node;
    }

    public NodeInfo getNodeForRead(String key) {
        NodeInfo master = hashRing.getNodeForKey(key);
        if (master == null) throw new IllegalStateException("No nodes available in cluster");

        Set<NodeInfo> replicaSet = replicaManager.getReplicas(master);
        if (replicaSet.isEmpty()) {
            log.trace("No replicas for master {}, using master for read", master.nodeId());
            return master;
        }

        List<NodeInfo> readPool = new ArrayList<>(replicaSet.size() + 1);
        readPool.add(master);
        readPool.addAll(replicaSet);
        int index = ThreadLocalRandom.current().nextInt(readPool.size());
        NodeInfo selected = readPool.get(index);
        log.trace("Selected node {} for read (key={}, pool size={})", selected.nodeId(), key, readPool.size());
        return selected;
    }

    @Deprecated
    public NodeInfo getNodeForKey(String key) {
        return getMasterNodeForKey(key);
    }

    public Set<NodeInfo> getAllNodes() {
        return hashRing.getNodes();
    }

    public int getVirtualNodeCount() {
        return config.getVirtualNodes();
    }

    public Set<NodeInfo> getReplicas(NodeInfo master) {
        return replicaManager.getReplicas(master);
    }

    private boolean isMasterNode(NodeInfo node) {
        return !node.nodeId().toLowerCase().contains("replica");
    }

    private String extractMasterNodeId(String replicaNodeId) {
        if (replicaNodeId.matches("replica-\\d+")) {
            String masterNodeId = replicaNodeId.replace("replica-", "master-");
            log.debug("Extracted master nodeId '{}' from replica '{}' (simple convention)", masterNodeId, replicaNodeId);
            return masterNodeId;
        }

        String masterNodeId = replicaNodeId
                .replaceAll("replica[-_]?\\d*[-_]?", "")
                .replaceAll("[-_]?replica\\d*", "")
                .replaceAll("^[-_]+|[-_]+$", "");

        if (masterNodeId.isEmpty()) throw new IllegalStateException("Cannot extract master nodeId from replica: " + replicaNodeId);
        log.debug("Extracted master nodeId '{}' from replica '{}'", masterNodeId, replicaNodeId);
        return masterNodeId;
    }

    public synchronized void removeMaster(NodeInfo failedMaster) {
        if (failedMaster == null) {
            log.warn("Attempted to remove null master node");
            return;
        }

        boolean removed = hashRing.removeNode(failedMaster);
        if (removed) {
            log.warn("Removed failed master from hash ring: {} (host={}:{})", failedMaster.nodeId(), failedMaster.host(), failedMaster.port());
        } else {
            log.warn("Master {} not found in hash ring, might have been already removed", failedMaster.nodeId());
        }
    }

    public synchronized void addMaster(NodeInfo promotedReplica) {
        if (promotedReplica == null) {
            log.warn("Attempted to add null master node");
            return;
        }

        hashRing.addNode(promotedReplica);
        log.warn("Promoted replica to master in hash ring: {} (host={}:{})", promotedReplica.nodeId(), promotedReplica.host(), promotedReplica.port());
    }

    public synchronized void restoreMaster(NodeInfo recoveredMaster) {
        if (recoveredMaster == null) {
            log.warn("Attempted to restore null master node");
            return;
        }
        hashRing.addNode(recoveredMaster);
        log.info("Restored recovered master to hash ring: {} (host={}:{})", recoveredMaster.nodeId(), recoveredMaster.host(), recoveredMaster.port());
    }
}