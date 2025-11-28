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

@RestController
@RequestMapping("/api/cluster")
public class ClusterController {

    private final ClusterManager clusterManager;

    public ClusterController(ClusterManager clusterManager) {
        this.clusterManager = clusterManager;
    }

    @GetMapping("/masters")
    public ResponseEntity<Map<String, Object>> getMasterNodes() {
        Set<NodeInfo> masters = clusterManager.getAllNodes();

        List<Map<String, Object>> mastersList = masters.stream()
                .map(node -> {
                    Map<String, Object> nodeMap = new HashMap<>();
                    nodeMap.put("nodeId", node.nodeId());
                    nodeMap.put("host", node.host());
                    nodeMap.put("port", node.port());
                    nodeMap.put("replicationPort", node.replicationPort());
                    return nodeMap;
                })
                .collect(Collectors.toList());

        Map<String, Object> response = new HashMap<>();
        response.put("masters", mastersList);
        response.put("totalMasters", masters.size());
        response.put("virtualNodesPerMaster", clusterManager.getVirtualNodeCount());
        return ResponseEntity.ok(response);
    }

    @GetMapping("/topology")
    public ResponseEntity<Map<String, Object>> getClusterTopology() {
        Set<NodeInfo> masters = clusterManager.getAllNodes();

        List<Map<String, Object>> topology = masters.stream()
                .map(master -> {
                    Set<NodeInfo> replicas = clusterManager.getReplicas(master);

                    List<Map<String, Object>> replicasList = replicas.stream()
                            .map(replica -> {
                                Map<String, Object> replicaMap = new HashMap<>();
                                replicaMap.put("nodeId", replica.nodeId());
                                replicaMap.put("host", replica.host());
                                replicaMap.put("port", replica.port());
                                replicaMap.put("replicationPort", replica.replicationPort());
                                return replicaMap;
                            }).toList();

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
                .map(replica -> {
                    Map<String, Object> replicaMap = new HashMap<>();
                    replicaMap.put("nodeId", replica.nodeId());
                    replicaMap.put("host", replica.host());
                    replicaMap.put("port", replica.port());
                    return replicaMap;
                }).toList();

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
