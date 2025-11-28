package com.iksanov.distributedcache.common.cluster.sharding;

import com.iksanov.distributedcache.common.cluster.NodeInfo;
import com.iksanov.distributedcache.common.util.HashUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class ConsistentHashRing {

    private static final Logger log = LoggerFactory.getLogger(ConsistentHashRing.class);
    private final ConcurrentSkipListMap<Integer, NodeInfo> ring = new ConcurrentSkipListMap<>();
    private final Map<String, List<Integer>> nodeToHashes = new ConcurrentHashMap<>();
    private final int virtualNodeCount;

    public ConsistentHashRing(int virtualNodeCount) {
        if (virtualNodeCount <= 0) throw new IllegalArgumentException("virtualNodeCount must be > 0");
        this.virtualNodeCount = virtualNodeCount;
    }

    public synchronized void addNode(NodeInfo node) {
        Objects.requireNonNull(node, "node is null");
        String nodeId = node.nodeId();
        if (nodeToHashes.containsKey(nodeId)) {
            log.warn("Node {} already exists in ring", nodeId);
            return;
        }
        List<Integer> hashes = new ArrayList<>(virtualNodeCount);
        for (int i = 0; i < virtualNodeCount; i++) {
            String vnodeKey = nodeId + "#" + i;
            int hash = HashUtils.hash(vnodeKey);
            while (ring.containsKey(hash)) {
                hash = (hash + 1) & 0x7FFFFFFF;
            }
            ring.put(hash, node);
            hashes.add(hash);
        }
        nodeToHashes.put(nodeId, hashes);
        log.debug("Added node {} with {} virtual nodes", nodeId, virtualNodeCount);
    }

    public synchronized boolean removeNode(NodeInfo node) {
        Objects.requireNonNull(node, "node is null");
        String nodeId = node.nodeId();
        List<Integer> hashes = nodeToHashes.remove(nodeId);
        if (hashes == null) {
            log.warn("Node {} not found in ring", nodeId);
            return false;
        }
        for (int h : hashes) ring.remove(h);
        log.debug("Removed node {} ({} virtual nodes)", nodeId, hashes.size());
        return true;
    }

    public NodeInfo getNodeForKey(String key) {
        Objects.requireNonNull(key, "key is null");
        if (ring.isEmpty()) return null;
        int hash = HashUtils.hash(key);
        Map.Entry<Integer, NodeInfo> entry = ring.ceilingEntry(hash);
        if (entry == null) entry = ring.firstEntry();
        return entry.getValue();
    }

    public Set<NodeInfo> getNodes() {
        return new HashSet<>(ring.values());
    }

    public int ringSize() {
        return ring.size();
    }

    public SortedMap<Integer, NodeInfo> snapshot() {
        return Collections.unmodifiableSortedMap(ring);
    }
}
