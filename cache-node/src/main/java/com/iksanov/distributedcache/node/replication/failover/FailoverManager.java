package com.iksanov.distributedcache.node.replication.failover;

import com.iksanov.distributedcache.common.cluster.NodeInfo;
import com.iksanov.distributedcache.node.config.ApplicationConfig.NodeRole;
import com.iksanov.distributedcache.node.replication.ReplicationSender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * FailoverManager implements simple priority-based failover mechanism.
 *
 * <p>Design:
 * <ul>
 *   <li>MASTER: Sends periodic heartbeats to replicas</li>
 *   <li>REPLICA (priority=1): Monitors lastReplicationTime, promotes to master if no heartbeat for 3+ seconds</li>
 *   <li>Uses existing replication channel for heartbeats (no extra overhead)</li>
 * </ul>
 */
public class FailoverManager {

    private static final Logger log = LoggerFactory.getLogger(FailoverManager.class);
    private static final long HEARTBEAT_INTERVAL_MS = 1000;
    private static final long HEARTBEAT_TIMEOUT_MS = 3000;
    private final String nodeId;
    private final int priority;
    private final AtomicReference<NodeRole> currentRole;
    private final ReplicationSender replicationSender;
    private final NodeInfo selfNode;
    private final NodeInfo masterNode;
    private final AtomicLong lastReplicationTime = new AtomicLong(System.currentTimeMillis());
    private final AtomicLong epoch = new AtomicLong(0);
    private ScheduledExecutorService scheduler;
    private volatile boolean running = false;
    private volatile RoleChangeListener roleChangeListener;

    @FunctionalInterface
    public interface RoleChangeListener {
        void onRoleChanged(NodeRole oldRole, NodeRole newRole, long newEpoch);
    }

    public FailoverManager(String nodeId, int priority, NodeRole initialRole, ReplicationSender replicationSender, NodeInfo selfNode, NodeInfo masterNode) {
        this.nodeId = Objects.requireNonNull(nodeId, "nodeId cannot be null");
        this.priority = priority;
        this.currentRole = new AtomicReference<>(Objects.requireNonNull(initialRole, "initialRole cannot be null"));
        this.replicationSender = Objects.requireNonNull(replicationSender, "replicationSender cannot be null");
        this.selfNode = selfNode;
        this.masterNode = masterNode;

        if (initialRole == NodeRole.REPLICA && masterNode == null) log.warn("REPLICA node created without masterNode - failover monitoring will not work");
        if (initialRole == NodeRole.MASTER && selfNode == null) log.warn("MASTER node created without selfNode - heartbeat sending will not work");
    }

    public synchronized void start() {
        if (running) {
            log.warn("FailoverManager already running");
            return;
        }

        scheduler = Executors.newScheduledThreadPool(1, runnable -> {
            Thread thread = new Thread(runnable);
            thread.setName("failover-" + nodeId);
            thread.setDaemon(true);
            return thread;
        });

        if (currentRole.get() == NodeRole.MASTER) {
            startMasterHeartbeat();
        } else if (currentRole.get() == NodeRole.REPLICA && priority == 1) {
            startReplicaMonitoring();
        }

        running = true;
        log.info("FailoverManager started for {} node (priority={})", currentRole.get(), priority);
    }

    public synchronized void stop() {
        if (!running) return;

        running = false;
        if (scheduler != null) {
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(2, TimeUnit.SECONDS)) scheduler.shutdownNow();
            } catch (InterruptedException e) {
                scheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        log.info("FailoverManager stopped");
    }

    private void startMasterHeartbeat() {
        scheduler.scheduleAtFixedRate(() -> {
            try {
                if (currentRole.get() == NodeRole.MASTER && selfNode != null) {
                    log.trace("Sending HEARTBEAT to replicas");
                    replicationSender.sendHeartbeatToReplicas(selfNode);
                }
            } catch (Exception e) {
                log.error("Failed to send heartbeat: {}", e.getMessage(), e);
            }
        }, HEARTBEAT_INTERVAL_MS, HEARTBEAT_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    private void startReplicaMonitoring() {
        scheduler.scheduleAtFixedRate(() -> {
            try {
                if (currentRole.get() != NodeRole.REPLICA) return;
                long timeSinceLastReplication = System.currentTimeMillis() - lastReplicationTime.get();
                if (timeSinceLastReplication > HEARTBEAT_TIMEOUT_MS) {
                    log.warn("Master heartbeat timeout ({}ms > {}ms), promoting to MASTER", timeSinceLastReplication, HEARTBEAT_TIMEOUT_MS);
                    promoteToMaster();
                } else {
                    log.trace("Heartbeat OK (last replication: {}ms ago)", timeSinceLastReplication);
                }
            } catch (Exception e) {
                log.error("Failed to monitor heartbeat: {}", e.getMessage(), e);
            }
        }, HEARTBEAT_TIMEOUT_MS, HEARTBEAT_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    private void promoteToMaster() {
        long newEpoch = epoch.incrementAndGet();
        promoteToMasterWithEpoch(newEpoch, java.util.Collections.emptyList());
    }

    public synchronized void promoteToMasterWithEpoch(long newEpoch) {
        promoteToMasterWithEpoch(newEpoch, java.util.Collections.emptyList());
    }

    public synchronized void promoteToMasterWithEpoch(long newEpoch, List<NodeInfo> replicas) {
        NodeRole oldRole = currentRole.get();
        if (oldRole == NodeRole.MASTER) {
            log.debug("Already MASTER, skipping promotion");
            return;
        }

        epoch.set(newEpoch);
        currentRole.set(NodeRole.MASTER);

        log.info("========================================");
        log.info("PROMOTED TO MASTER");
        log.info("  Node: {}", nodeId);
        log.info("  Priority: {}", priority);
        log.info("  Epoch: {}", newEpoch);
        log.info("  Replicas to register: {}", replicas != null ? replicas.size() : 0);
        log.info("  Source: {}", (oldRole == NodeRole.REPLICA ? "Self-promotion" : "Proxy command"));
        log.info("========================================");

        if (replicas != null && !replicas.isEmpty() && selfNode != null && replicationSender != null) {
            try {
                replicationSender.updateReplicaTargets(selfNode, replicas);
                log.info("Replication chain updated: {} replicas registered", replicas.size());
            } catch (Exception e) {
                log.error("Failed to update replication targets: {}", e.getMessage(), e);
            }
        } else if (replicas == null || replicas.isEmpty()) {
            log.info("No replicas to register (standalone promotion)");
        }

        if (scheduler != null && !scheduler.isShutdown()) {
            scheduler.shutdown();
            scheduler = Executors.newScheduledThreadPool(1, runnable -> {
                Thread thread = new Thread(runnable);
                thread.setName("failover-" + nodeId + "-master");
                thread.setDaemon(true);
                return thread;
            });
            startMasterHeartbeat();
        }

        if (roleChangeListener != null) {
            try {
                roleChangeListener.onRoleChanged(oldRole, NodeRole.MASTER, newEpoch);
            } catch (Exception e) {
                log.error("Error in role change listener: {}", e.getMessage(), e);
            }
        }
    }

    public void onReplicationReceived() {
        lastReplicationTime.set(System.currentTimeMillis());
        log.trace("Replication received, updated lastReplicationTime");
    }

    public void onHeartbeatAck(long masterEpoch) {
        log.trace("Received heartbeat ACK with epoch={}", masterEpoch);
        lastReplicationTime.set(System.currentTimeMillis());
    }

    public void setRoleChangeListener(RoleChangeListener listener) {
        this.roleChangeListener = listener;
    }

    public NodeRole getCurrentRole() {
        return currentRole.get();
    }

    public long getEpoch() {
        return epoch.get();
    }

    public boolean isRunning() {
        return running;
    }
}