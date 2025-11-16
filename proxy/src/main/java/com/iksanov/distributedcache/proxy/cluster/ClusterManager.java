package com.iksanov.distributedcache.proxy.cluster;

import com.iksanov.distributedcache.common.cluster.NodeInfo;
import com.iksanov.distributedcache.common.cluster.ConsistentHashRing;
import com.iksanov.distributedcache.proxy.config.CacheClusterConfig;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Set;

@Component
public class ClusterManager {

    private static final Logger log = LoggerFactory.getLogger(ClusterManager.class);
    private final ConsistentHashRing hashRing;
    private final CacheClusterConfig config;

    public ClusterManager(CacheClusterConfig config) {
        this.config = config;
        this.hashRing = new ConsistentHashRing(config.getVirtualNodes());
    }

    @PostConstruct
    public void init() {
        List<String> nodeAddresses = config.getNodes();
        if (nodeAddresses.isEmpty()) throw new IllegalStateException("No cache nodes configured");

        log.info("Initializing cluster with {} nodes", nodeAddresses.size());
        
        for (String nodeAddr : nodeAddresses) {
            try {
                NodeInfo node = NodeInfo.fromString(nodeAddr);
                hashRing.addNode(node);
                log.info("Added node to cluster: {}", node);
            } catch (Exception e) {
                log.error("Failed to parse node address '{}': {}", nodeAddr, e.getMessage());
                throw new IllegalStateException("Invalid node configuration: " + nodeAddr, e);
            }
        }

        log.info("Cluster initialized with {} nodes, {} virtual nodes per node", hashRing.getNodes().size(), config.getVirtualNodes());
    }

    public NodeInfo getNodeForKey(String key) {
        NodeInfo node = hashRing.getNodeForKey(key);
        if (node == null) throw new IllegalStateException("No nodes available in cluster");
        return node;
    }

    public Set<NodeInfo> getAllNodes() {
        return hashRing.getNodes();
    }

    public int getVirtualNodeCount() {
        return config.getVirtualNodes();
    }
}