package com.iksanov.distributedcache.node.net;

import com.iksanov.distributedcache.common.dto.CacheRequest;
import com.iksanov.distributedcache.common.dto.CacheResponse;
import com.iksanov.distributedcache.common.exception.CacheException;
import com.iksanov.distributedcache.node.config.ApplicationConfig;
import com.iksanov.distributedcache.node.config.ApplicationConfig.NodeRole;
import com.iksanov.distributedcache.node.core.CacheStore;
import com.iksanov.distributedcache.node.metrics.NetMetrics;
import com.iksanov.distributedcache.node.replication.ReplicationManager;
import com.iksanov.distributedcache.node.replication.failover.FailoverManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

/**
 * RequestProcessor is responsible for processing {@link CacheRequest} objects
 * and producing corresponding {@link CacheResponse} results.
 * <p>
 * This class isolates the network layer (Netty) from the core cache logic (CacheStore).
 * It is thread-safe, production-ready, and designed for extension:
 *  - Validates incoming requests
 *  - Enforces role-based access control (replicas cannot accept writes)
 *  - Converts exceptions into CacheResponse errors
 *  - Logs performance and error details
 *  - Can be extended later with metrics, replication, or async processing
 */
public class RequestProcessor {

    private static final Logger log = LoggerFactory.getLogger(RequestProcessor.class);
    private final CacheStore store;
    private final ReplicationManager replicationManager;
    private final FailoverManager failoverManager;
    private volatile NodeRole nodeRole;
    private static final long SLOW_REQUEST_THRESHOLD_MS = 100;
    private final NetMetrics metrics;

    public RequestProcessor(CacheStore store, ReplicationManager replicationManager, NodeRole nodeRole, NetMetrics metrics, FailoverManager failoverManager) {
        this.store = Objects.requireNonNull(store, "store");
        this.replicationManager = replicationManager;
        this.nodeRole = Objects.requireNonNull(nodeRole, "nodeRole");
        this.metrics = Objects.requireNonNull(metrics, "metrics");
        this.failoverManager = failoverManager;
    }

    public CacheResponse process(CacheRequest request) {
        Objects.requireNonNull(request, "request");
        Instant start = Instant.now();
        metrics.incrementRequests();
        try {
            return switch (request.command()) {
                case GET -> handleGet(request);
                case SET -> handleSet(request);
                case DELETE -> handleDelete(request);
                case STATUS -> handleStatus(request);
                case PROMOTE -> handlePromote(request);
            };
        } catch (CacheException ce) {
            metrics.incrementErrors();
            log.error("CacheException while processing requestId={}: {}", request.requestId(), ce.getMessage());
            return CacheResponse.error(request.requestId(), ce.getMessage());
        } catch (Exception e) {
            metrics.incrementErrors();
            log.error("Unexpected error while processing requestId={}", request.requestId(), e);
            return CacheResponse.error(request.requestId(), "Internal server error");
        } finally {
            long durationMs = Duration.between(start, Instant.now()).toMillis();
            metrics.recordRequestDuration(durationMs);
            metrics.incrementResponses();
            if (durationMs > SLOW_REQUEST_THRESHOLD_MS) {
                log.warn("Slow request [command={}, requestId={}] took {} ms", request.command(), request.requestId(), durationMs);
            } else {
                log.debug("Request [command={}, requestId={}] processed in {} ms", request.command(), request.requestId(), durationMs);
            }
        }
    }

    private CacheResponse handleGet(CacheRequest request) {
        String key = requireKey(request);
        String value = store.get(key);
        if (value == null) {
            log.debug("GET miss for key={}", key);
            return CacheResponse.notFound(request.requestId());
        }
        log.trace("GET hit for key={} valueLength={}", key, value.length());
        return CacheResponse.ok(request.requestId(), value);
    }

    private CacheResponse handleSet(CacheRequest request) {
        if (nodeRole == NodeRole.REPLICA) {
            log.warn("Rejecting SET request on REPLICA node for key={}", request.key());
            return CacheResponse.error(request.requestId(), "Cannot write to replica node");
        }

        String key = requireKey(request);
        String value = requireValue(request);
        store.put(key, value);
        log.trace("SET key={} valueLength={}", key, value.length());
        try {
            replicationManager.onLocalSet(key, value);
        } catch (Exception e) {
            metrics.incrementErrors();
            log.error("ReplicationManager.onLocalSet failed for key={}: {}", key, e.getMessage(), e);
        }
        return CacheResponse.ok(request.requestId(), "OK");
    }

    private CacheResponse handleDelete(CacheRequest request) {
        if (nodeRole == NodeRole.REPLICA) {
            log.warn("Rejecting DELETE request on REPLICA node for key={}", request.key());
            return CacheResponse.error(request.requestId(), "Cannot write to replica node");
        }

        String key = requireKey(request);
        store.delete(key);
        log.trace("DELETE key={}", key);
        try {
            replicationManager.onLocalDelete(key);
        } catch (Exception e) {
            metrics.incrementErrors();
            log.error("ReplicationManager.onLocalDelete failed for key={}: {}", key, e.getMessage(), e);
        }
        return CacheResponse.ok(request.requestId(), "OK");
    }

    private String requireKey(CacheRequest request) {
        String key = request.key();
        if (key == null || key.isBlank()) throw new CacheException("Key must not be null or blank");
        return key;
    }

    private String requireValue(CacheRequest request) {
        String value = request.value();
        if (value == null) throw new CacheException("Value must not be null for SET");
        return value;
    }

    public void updateRole(NodeRole newRole) {
        NodeRole oldRole = this.nodeRole;
        this.nodeRole = Objects.requireNonNull(newRole, "newRole");
        log.info("RequestProcessor role updated: {} -> {}", oldRole, newRole);
    }

    public NodeRole getCurrentRole() {
        return nodeRole;
    }

    private CacheResponse handleStatus(CacheRequest request) {
        if (failoverManager == null) {
            // Standalone mode - no failover manager
            String metadata = String.format("role:%s,epoch:0", nodeRole);
            log.trace("STATUS request: {}", metadata);
            return CacheResponse.okWithMetadata(request.requestId(), metadata);
        }

        NodeRole currentRole = failoverManager.getCurrentRole();
        long currentEpoch = failoverManager.getEpoch();
        String metadata = String.format("role:%s,epoch:%d", currentRole, currentEpoch);
        log.debug("STATUS request: {}", metadata);
        return CacheResponse.okWithMetadata(request.requestId(), metadata);
    }

    private CacheResponse handlePromote(CacheRequest request) {
        if (failoverManager == null) {
            log.warn("Received PROMOTE command but node is in standalone mode (no failover manager)");
            return CacheResponse.error(request.requestId(), "Node does not support failover (standalone mode)");
        }

        long requestedEpoch;
        java.util.List<com.iksanov.distributedcache.common.cluster.NodeInfo> replicaList = new java.util.ArrayList<>();

        try {
            String value = request.value();
            if (value == null || !value.contains("epoch:")) {
                return CacheResponse.error(request.requestId(), "Invalid PROMOTE request format. Expected 'epoch:N[,replicas:...]'");
            }

            String[] parts = value.split(",", 2);

            String epochPart = parts[0];
            if (!epochPart.startsWith("epoch:")) {
                return CacheResponse.error(request.requestId(), "Invalid PROMOTE format: missing epoch");
            }
            requestedEpoch = Long.parseLong(epochPart.substring(6));

            if (parts.length > 1 && parts[1].startsWith("replicas:")) {
                String replicasPart = parts[1].substring(9);
                if (!replicasPart.isEmpty()) {
                    String[] replicaStrings = replicasPart.split(";");
                    for (String replicaStr : replicaStrings) {
                        try {
                            com.iksanov.distributedcache.common.cluster.NodeInfo replica =
                                com.iksanov.distributedcache.common.cluster.NodeInfo.fromString(replicaStr);
                            replicaList.add(replica);
                        } catch (Exception e) {
                            log.warn("Failed to parse replica '{}': {}", replicaStr, e.getMessage());
                        }
                    }
                    log.info("PROMOTE command includes {} replicas: {}",
                        replicaList.size(),
                        replicaList.stream().map(com.iksanov.distributedcache.common.cluster.NodeInfo::nodeId).toList());
                }
            }
        } catch (NumberFormatException e) {
            return CacheResponse.error(request.requestId(), "Invalid epoch format in PROMOTE request");
        } catch (Exception e) {
            log.error("Failed to parse PROMOTE request: {}", e.getMessage(), e);
            return CacheResponse.error(request.requestId(), "Invalid PROMOTE request format: " + e.getMessage());
        }
        long currentEpoch = failoverManager.getEpoch();
        NodeRole currentRole = failoverManager.getCurrentRole();
        if (currentRole == NodeRole.MASTER && currentEpoch >= requestedEpoch) {
            log.info("PROMOTE command ignored: already MASTER with epoch={} (requested={})",
                currentEpoch, requestedEpoch);
            String metadata = String.format("role:%s,epoch:%d", currentRole, currentEpoch);
            return CacheResponse.okWithMetadata(request.requestId(), metadata);
        }

        if (requestedEpoch <= currentEpoch) {
            log.warn("PROMOTE command rejected: requested epoch={} <= current epoch={}",
                requestedEpoch, currentEpoch);
            return CacheResponse.error(request.requestId(),
                String.format("Epoch too old. Current: %d, Requested: %d", currentEpoch, requestedEpoch));
        }

        log.warn("========================================");
        log.warn("PROMOTE command received from proxy");
        log.warn("  Current: role={}, epoch={}", currentRole, currentEpoch);
        log.warn("  Requested: epoch={}", requestedEpoch);
        log.warn("  Replicas to register: {}", replicaList.size());
        log.warn("  Action: Promoting to MASTER");
        log.warn("========================================");

        try {
            failoverManager.promoteToMasterWithEpoch(requestedEpoch, replicaList);
            NodeRole newRole = failoverManager.getCurrentRole();
            long newEpoch = failoverManager.getEpoch();
            String metadata = String.format("role:%s,epoch:%d", newRole, newEpoch);
            log.info("PROMOTE successful: {}", metadata);
            return CacheResponse.okWithMetadata(request.requestId(), metadata);
        } catch (Exception e) {
            log.error("PROMOTE failed: {}", e.getMessage(), e);
            return CacheResponse.error(request.requestId(), "Promotion failed: " + e.getMessage());
        }
    }
}
