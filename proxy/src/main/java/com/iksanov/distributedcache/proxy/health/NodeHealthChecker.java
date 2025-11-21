package com.iksanov.distributedcache.proxy.health;

import com.iksanov.distributedcache.common.cluster.NodeInfo;
import com.iksanov.distributedcache.common.dto.CacheRequest;
import com.iksanov.distributedcache.common.dto.CacheResponse;
import com.iksanov.distributedcache.proxy.client.CacheClient;
import com.iksanov.distributedcache.proxy.cluster.ClusterManager;
import com.iksanov.distributedcache.proxy.metrics.CacheMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.*;

/**
 * Periodically checks health of all master nodes and performs automatic failover.
 * <p>
 * Design:
 * - Lightweight PING using simple GET request with short timeout
 * - Runs every 5 seconds (configurable)
 * - On master failure: automatically promotes replica to master in hashRing
 * - Minimal overhead: single-threaded executor, fast ping timeout (500ms)
 */
@Component
public class NodeHealthChecker {

    private static final Logger log = LoggerFactory.getLogger(NodeHealthChecker.class);
    private static final int HEALTH_CHECK_INTERVAL_MS = 5000;
    private static final int PING_TIMEOUT_MS = 500;
    private static final int CONSECUTIVE_FAILURES_THRESHOLD = 2;
    private static final String PING_KEY = "__health_check__";
    private final ClusterManager clusterManager;
    private final CacheClient cacheClient;
    private final CacheMetrics metrics;
    private final Map<String, Integer> failureCount = new ConcurrentHashMap<>();
    private final Set<String> failedMasters = ConcurrentHashMap.newKeySet();

    public NodeHealthChecker(ClusterManager clusterManager, CacheClient cacheClient, CacheMetrics metrics) {
        this.clusterManager = clusterManager;
        this.cacheClient = cacheClient;
        this.metrics = metrics;
    }

    @Scheduled(fixedRate = HEALTH_CHECK_INTERVAL_MS)
    public void checkClusterHealth() {
        Set<NodeInfo> allMasters = clusterManager.getAllNodes();

        if (allMasters.isEmpty()) {
            log.warn("No master nodes configured, skipping health check");
            return;
        }

        log.trace("Running health check for {} master nodes", allMasters.size());

        for (NodeInfo master : allMasters) {
            if (failedMasters.contains(master.nodeId())) {
                log.trace("Skipping health check for already failed master: {}", master.nodeId());
                continue;
            }

            boolean healthy = pingNode(master);

            if (healthy) {
                failureCount.remove(master.nodeId());
                log.trace("Master {} is healthy", master.nodeId());
            } else {
                handleUnhealthyMaster(master);
            }
        }
    }

    private NodeStatus getNodeStatus(NodeInfo node) {
        try {
            CacheRequest statusRequest = new CacheRequest(
                    UUID.randomUUID().toString(),
                    System.currentTimeMillis(),
                    CacheRequest.Command.STATUS,
                    null,
                    null
            );

            CompletableFuture<CacheResponse> future = cacheClient.sendRequest(node, statusRequest);
            CacheResponse response = future.get(PING_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            if (response.status() == CacheResponse.Status.OK && response.metadata() != null) return parseNodeStatus(response.metadata());
            log.debug("STATUS request to {} returned invalid response", node.nodeId());
            return null;
        } catch (TimeoutException e) {
            log.debug("STATUS timeout for node {}", node.nodeId());
            return null;
        } catch (Exception e) {
            log.debug("STATUS failed for node {}: {}", node.nodeId(), e.getMessage());
            return null;
        }
    }

    private boolean pingNode(NodeInfo node) {
        NodeStatus status = getNodeStatus(node);
        return status != null;
    }

    private NodeStatus parseNodeStatus(String metadata) {
        try {
            String[] parts = metadata.split(",");
            String role = null;
            long epoch = 0;
            for (String part : parts) {
                String[] kv = part.split(":");
                if (kv.length == 2) {
                    if ("role".equals(kv[0])) {
                        role = kv[1];
                    } else if ("epoch".equals(kv[0])) {
                        epoch = Long.parseLong(kv[1]);
                    }
                }
            }
            if (role != null) return new NodeStatus(role, epoch);
        } catch (Exception e) {
            log.warn("Failed to parse node status from metadata: {}", metadata, e);
        }
        return null;
    }

    private boolean promoteReplica(NodeInfo replica, NodeInfo failedMaster, long newEpoch) {
        try {
            log.info("Sending PROMOTE command to {} with epoch={}", replica.nodeId(), newEpoch);
            Set<NodeInfo> allReplicas = clusterManager.getReplicas(failedMaster);
            List<NodeInfo> otherReplicas = allReplicas.stream()
                    .filter(r -> !r.nodeId().equals(replica.nodeId()))
                    .toList();

            StringBuilder valueBuilder = new StringBuilder();
            valueBuilder.append("epoch:").append(newEpoch);
            if (!otherReplicas.isEmpty()) {
                valueBuilder.append(",replicas:");
                for (int i = 0; i < otherReplicas.size(); i++) {
                    if (i > 0) {
                        valueBuilder.append(";");
                    }
                    NodeInfo otherReplica = otherReplicas.get(i);
                    valueBuilder.append(otherReplica.nodeId())
                            .append(":")
                            .append(otherReplica.host())
                            .append(":")
                            .append(otherReplica.port())
                            .append(":")
                            .append(otherReplica.replicationPort());
                }
                log.info("Promoting {} with {} other replicas: {}", replica.nodeId(), otherReplicas.size(), otherReplicas.stream().map(NodeInfo::nodeId).toList());
            } else {
                log.info("Promoting {} without other replicas (standalone)", replica.nodeId());
            }

            CacheRequest promoteRequest = new CacheRequest(
                    UUID.randomUUID().toString(),
                    System.currentTimeMillis(),
                    CacheRequest.Command.PROMOTE,
                    null,
                    valueBuilder.toString()
            );

            CompletableFuture<CacheResponse> future = cacheClient.sendRequest(replica, promoteRequest);
            CacheResponse response = future.get(PING_TIMEOUT_MS * 2, TimeUnit.MILLISECONDS); // Double timeout for promotion
            if (response.status() == CacheResponse.Status.OK) {
                log.info("PROMOTE successful for {}: {}", replica.nodeId(), response.metadata());
                return true;
            } else {
                log.error("PROMOTE failed for {}: {}", replica.nodeId(), response.errorMessage());
                return false;
            }
        } catch (TimeoutException e) {
            log.error("PROMOTE timeout for node {}", replica.nodeId());
            return false;
        } catch (Exception e) {
            log.error("PROMOTE failed for node {}: {}", replica.nodeId(), e.getMessage());
            return false;
        }
    }

    private record NodeStatus(String role, long epoch) {
        public boolean isMaster() {
            return "MASTER".equals(role);
        }
        public boolean isReplica() {
            return "REPLICA".equals(role);
        }
    }

    private void handleUnhealthyMaster(NodeInfo master) {
        int failures = failureCount.compute(master.nodeId(), (k, v) -> (v == null ? 0 : v) + 1);
        metrics.recordHealthCheckFailure();
        log.warn("Master {} health check failed ({}/{} consecutive failures)", master.nodeId(), failures, CONSECUTIVE_FAILURES_THRESHOLD);
        if (failures >= CONSECUTIVE_FAILURES_THRESHOLD) {
            log.error("Master {} is down (failed {} consecutive health checks), triggering failover", master.nodeId(), failures);
            performFailover(master);
        }
    }

    private void performFailover(NodeInfo failedMaster) {
        failedMasters.add(failedMaster.nodeId());
        log.warn("========================================");
        log.warn("FAILOVER TRIGGERED");
        log.warn("  Failed Master: {}", failedMaster.nodeId());
        log.warn("========================================");
        clusterManager.removeMaster(failedMaster);
        log.info("Removed failed master {} from hash ring", failedMaster.nodeId());
        Set<NodeInfo> replicas = clusterManager.getReplicas(failedMaster);
        if (replicas.isEmpty()) {
            log.error("No replicas available for failed master {}, cannot perform failover!", failedMaster.nodeId());
            return;
        }

        List<NodeInfo> sortedReplicas = new ArrayList<>(replicas);
        sortedReplicas.sort(Comparator.comparing(NodeInfo::nodeId));
        NodeInfo promotedReplica = null;
        NodeStatus replicaStatus = null;
        for (NodeInfo replica : sortedReplicas) {
            NodeStatus status = getNodeStatus(replica);
            if (status != null) {
                log.info("Replica {} status: role={}, epoch={}", replica.nodeId(), status.role(), status.epoch());

                if (status.isMaster()) {
                    log.info("Replica {} already promoted to MASTER (epoch={})", replica.nodeId(), status.epoch());
                    promotedReplica = replica;
                    replicaStatus = status;
                    break;
                }
                promotedReplica = replica;
                replicaStatus = status;
                break;
            } else {
                log.warn("Replica {} is unhealthy, trying next replica", replica.nodeId());
            }
        }

        if (promotedReplica == null) {
            log.error("All replicas for master {} are unhealthy, failover failed!", failedMaster.nodeId());
            return;
        }

        if (replicaStatus != null && replicaStatus.isReplica()) {
            long newEpoch = System.currentTimeMillis();
            boolean promoted = promoteReplica(promotedReplica, failedMaster, newEpoch);
            if (!promoted) {
                log.error("Failed to promote replica {}", promotedReplica.nodeId());
                return;
            }
        }

        clusterManager.addMaster(promotedReplica);
        metrics.recordFailover();
        log.warn("========================================");
        log.warn("FAILOVER COMPLETED");
        log.warn("  Failed Master: {}", failedMaster.nodeId());
        log.warn("  Promoted Replica: {}", promotedReplica.nodeId());
        if (replicaStatus != null) log.warn("  Final Epoch: {}", replicaStatus.epoch());
        log.warn("========================================");
    }

    public void triggerHealthCheck() {
        log.info("Manual health check triggered");
        checkClusterHealth();
    }

    public void resetFailureTracking(String masterNodeId) {
        failureCount.remove(masterNodeId);
        failedMasters.remove(masterNodeId);
        log.info("Reset failure tracking for master: {}", masterNodeId);
    }

    public int getFailureCount(String masterNodeId) {
        return failureCount.getOrDefault(masterNodeId, 0);
    }

    public boolean isMarkedAsFailed(String masterNodeId) {
        return failedMasters.contains(masterNodeId);
    }
}
