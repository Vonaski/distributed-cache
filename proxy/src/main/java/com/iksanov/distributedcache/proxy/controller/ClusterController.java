package com.iksanov.distributedcache.proxy.controller;

import com.iksanov.distributedcache.common.cluster.NodeInfo;
import com.iksanov.distributedcache.proxy.cluster.ClusterManager;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Controller for cluster monitoring and diagnostics.
 * Provides endpoints to inspect the current state of the distributed cache cluster.
 */
@RestController
@RequestMapping("/api/cluster")
public class ClusterController {

    private final ClusterManager clusterManager;

    public ClusterController(ClusterManager clusterManager) {
        this.clusterManager = clusterManager;
    }

    /**
     * Get all master nodes in the consistent hash ring.
     *
     * @return List of master nodes with their connection details
     */
    @GetMapping("/masters")
    public ResponseEntity<Map<String, Object>> getMasterNodes() {
        Set<NodeInfo> masters = clusterManager.getAllNodes();

        List<Map<String, Object>> mastersList = masters.stream()
                .map(node -> Map.of(
                        "nodeId", node.nodeId(),
                        "host", node.host(),
                        "port", node.port(),
                        "replicationPort", node.replicationPort()
                ))
                .collect(Collectors.toList());

        return ResponseEntity.ok(Map.of(
                "masters", mastersList,
                "totalMasters", masters.size(),
                "virtualNodesPerMaster", clusterManager.getVirtualNodeCount()
        ));
    }

    /**
     * Get complete cluster topology (masters + replicas).
     *
     * @return Full cluster topology with master-replica mappings
     */
    @GetMapping("/topology")
    public ResponseEntity<Map<String, Object>> getClusterTopology() {
        Set<NodeInfo> masters = clusterManager.getAllNodes();

        List<Map<String, Object>> topology = masters.stream()
                .map(master -> {
                    Set<NodeInfo> replicas = clusterManager.getReplicas(master);

                    List<Map<String, Object>> replicasList = replicas.stream()
                            .map(replica -> Map.of(
                                    "nodeId", replica.nodeId(),
                                    "host", replica.host(),
                                    "port", replica.port(),
                                    "replicationPort", replica.replicationPort()
                            ))
                            .collect(Collectors.toList());

                    return Map.of(
                            "master", Map.of(
                                    "nodeId", master.nodeId(),
                                    "host", master.host(),
                                    "port", master.port(),
                                    "replicationPort", master.replicationPort()
                            ),
                            "replicas", replicasList,
                            "replicaCount", replicas.size()
                    );
                })
                .collect(Collectors.toList());

        int totalReplicas = topology.stream()
                .mapToInt(m -> ((List<?>) m.get("replicas")).size())
                .sum();

        return ResponseEntity.ok(Map.of(
                "topology", topology,
                "summary", Map.of(
                        "totalMasters", masters.size(),
                        "totalReplicas", totalReplicas,
                        "virtualNodesPerMaster", clusterManager.getVirtualNodeCount()
                )
        ));
    }

    /**
     * Get cluster health summary.
     *
     * @return Basic cluster health information
     */
    @GetMapping("/health")
    public ResponseEntity<Map<String, Object>> getClusterHealth() {
        Set<NodeInfo> masters = clusterManager.getAllNodes();

        int totalReplicas = masters.stream()
                .mapToInt(master -> clusterManager.getReplicas(master).size())
                .sum();

        boolean healthy = !masters.isEmpty();

        return ResponseEntity.ok(Map.of(
                "status", healthy ? "UP" : "DOWN",
                "masterNodes", masters.size(),
                "totalReplicas", totalReplicas,
                "virtualNodes", clusterManager.getVirtualNodeCount(),
                "healthChecks", Map.of(
                        "hasMasters", !masters.isEmpty(),
                        "hasReplicas", totalReplicas > 0
                )
        ));
    }

    /**
     * Get detailed information about which node handles a specific key.
     *
     * @param key The cache key to lookup
     * @return Information about which master handles the key
     */
    @GetMapping("/node-for-key")
    public ResponseEntity<Map<String, Object>> getNodeForKey(@RequestParam String key) {
        if (key == null || key.isBlank()) {
            return ResponseEntity.badRequest().body(Map.of(
                    "error", "Key parameter is required"
            ));
        }

        NodeInfo masterForWrite = clusterManager.getMasterNodeForKey(key);
        NodeInfo nodeForRead = clusterManager.getNodeForRead(key);
        Set<NodeInfo> replicas = clusterManager.getReplicas(masterForWrite);

        List<Map<String, Object>> replicasList = replicas.stream()
                .map(replica -> Map.of(
                        "nodeId", replica.nodeId(),
                        "host", replica.host(),
                        "port", replica.port()
                ))
                .collect(Collectors.toList());

        return ResponseEntity.ok(Map.of(
                "key", key,
                "writeTarget", Map.of(
                        "nodeId", masterForWrite.nodeId(),
                        "host", masterForWrite.host(),
                        "port", masterForWrite.port()
                ),
                "readTarget", Map.of(
                        "nodeId", nodeForRead.nodeId(),
                        "host", nodeForRead.host(),
                        "port", nodeForRead.port(),
                        "note", "Random selection from master + replicas"
                ),
                "availableReplicas", replicasList,
                "replicaCount", replicas.size()
        ));
    }
}
