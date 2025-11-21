package com.iksanov.distributedcache.node.app;

import com.iksanov.distributedcache.common.cluster.NodeInfo;
import com.iksanov.distributedcache.common.cluster.ReplicaManager;
import com.iksanov.distributedcache.node.config.ApplicationConfig;
import com.iksanov.distributedcache.node.config.ApplicationConfig.NodeRole;
import com.iksanov.distributedcache.node.core.InMemoryCacheStore;
import com.iksanov.distributedcache.node.replication.failover.FailoverManager;
import com.iksanov.distributedcache.node.metrics.CacheMetrics;
import com.iksanov.distributedcache.node.metrics.MetricsServer;
import com.iksanov.distributedcache.node.metrics.NetMetrics;
import com.iksanov.distributedcache.node.metrics.ReplicationMetrics;
import com.iksanov.distributedcache.node.net.NetServer;
import com.iksanov.distributedcache.node.replication.ReplicationManager;
import com.iksanov.distributedcache.node.replication.ReplicationReceiver;
import com.iksanov.distributedcache.node.replication.ReplicationSender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

/**
 * Main application class for CacheNode.
 * Supports both standalone and cluster modes with master-replica topology.
 */
public class CacheNodeApplication {

    private static final Logger log = LoggerFactory.getLogger(CacheNodeApplication.class);
    private final ApplicationConfig config;
    private InMemoryCacheStore cache;
    private NetServer netServer;
    private ReplicationManager replicationManager;
    private ReplicationReceiver replicationReceiver;
    private ReplicationSender replicationSender;
    private FailoverManager failoverManager;
    private final CountDownLatch shutdownLatch = new CountDownLatch(1);
    private CacheMetrics cacheMetrics;
    private MetricsServer metricsServer;
    private NetMetrics netMetrics;
    private ReplicationMetrics replicationMetrics;

    public CacheNodeApplication(ApplicationConfig config) {
        this.config = config;
    }

    public static void main(String[] args) {
        ApplicationConfig config = ApplicationConfig.fromEnv();

        log.info("========================================");
        log.info("Starting CacheNode: {}", config.nodeId());
        log.info("Role: {}, Port: {}, Cluster: {}", config.role(), config.port(), config.clusterEnabled());
        log.info("========================================");

        CacheNodeApplication app = new CacheNodeApplication(config);
        app.start();
        app.awaitShutdown();
    }

    public void start() {
        try {
            cacheMetrics = new CacheMetrics();
            netMetrics = new NetMetrics();
            replicationMetrics = new ReplicationMetrics();
            log.info("[OK] Metrics initialized");

            cache = new InMemoryCacheStore(config.cacheMaxSize(), config.cacheTtlMillis(), 5000, cacheMetrics);
            log.info("[OK] Cache initialized (size={})", config.cacheMaxSize());

            metricsServer = new MetricsServer(8081, cacheMetrics, netMetrics, replicationMetrics);
            metricsServer.start();
            log.info("[OK] Metrics server started on port 8081");

            if (config.clusterEnabled()) {
                replicationManager = initCluster();
                log.info("[OK] Cluster mode enabled (role={})", config.role());
            }

            netServer = new NetServer(config.toNetServerConfig(), cache, replicationManager, config.role(), netMetrics, failoverManager);
            netServer.start();
            log.info("[OK] Server listening on {}:{}", config.host(), config.port());

            Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown, "shutdown-hook"));

            log.info("========================================");
            log.info("[SUCCESS] CacheNode ready!");
            log.info("  Cache port: {}", config.port());
            log.info("  Metrics port: 8081");
            log.info("========================================");

        } catch (Exception e) {
            log.error("Startup failed", e);
            shutdown();
            System.exit(1);
        }
    }

    private ReplicationManager initCluster() {
        NodeInfo self = new NodeInfo(config.nodeId(), config.host(), config.port(), config.replicationPort());

        ReplicaManager replicaManager = new ReplicaManager();
        NodeInfo masterNodeForReplica = null;

        if (config.role() == NodeRole.MASTER) {
            log.info("Initializing as MASTER node...");
            log.info("Node: {}", self.nodeId());

            for (String nodeAddr : config.clusterNodes()) {
                try {
                    NodeInfo replica = NodeInfo.fromString(nodeAddr);
                    replicaManager.registerReplica(self, replica);
                    log.info("Registered replica: {}", replica.nodeId());
                } catch (Exception e) {
                    log.warn("Failed to parse replica '{}': {}", nodeAddr, e.getMessage());
                }
            }

            log.info("Replicas registered: {}", replicaManager.getReplicas(self).size());
        } else {
            log.info("Initializing as REPLICA node...");
            if (!config.clusterNodes().isEmpty()) {
                try {
                    masterNodeForReplica = NodeInfo.fromString(config.clusterNodes().getFirst());
                    log.info("  Connected to master: {}", masterNodeForReplica.nodeId());
                } catch (Exception e) {
                    log.error("Failed to connect to master: {}", e.getMessage());
                }
            } else {
                log.warn("REPLICA node started without master configuration!");
            }
        }

        replicationReceiver = new ReplicationReceiver(
                config.host(),
                config.replicationPort(),
                cache,
                10 * 1024 * 1024,
                config.nodeId(),
                replicationMetrics
        );

        replicationReceiver.start();
        log.info("[OK] ReplicationReceiver started on port {}", config.replicationPort());
        replicationSender = new ReplicationSender(replicaManager, null, replicationMetrics);
        log.info("[OK] ReplicationSender initialized");

        failoverManager = new FailoverManager(
                config.nodeId(),
                config.priority(),
                config.role(),
                replicationSender,
                self,
                masterNodeForReplica
        );

        failoverManager.setRoleChangeListener((oldRole, newRole, newEpoch) -> {
            log.info("========================================");
            log.info("FAILOVER: Role changed {} -> {}", oldRole, newRole);
            log.info("  New epoch: {}", newEpoch);
            log.info("========================================");

            if (netServer != null) {
                netServer.updateRole(newRole);
                log.info("NetServer role updated to {}", newRole);
            } else {
                log.warn("NetServer not initialized, role update will take effect on next startup");
            }
        });

        failoverManager.start();
        log.info("[OK] FailoverManager started");

        ReplicationManager replManager = new ReplicationManager(
                self,
                replicationSender,
                replicationReceiver,
                replicationMetrics
        );

        if (config.role() == NodeRole.REPLICA && config.priority() == 1) {
            return new ReplicationManager(
                    self,
                    replicationSender,
                    replicationReceiver,
                    replicationMetrics
            ) {
                @Override
                public void onReplicationReceived(com.iksanov.distributedcache.node.replication.ReplicationTask task) {
                    super.onReplicationReceived(task);
                    failoverManager.onReplicationReceived();
                }
            };
        }

        return replManager;
    }

    private void shutdown() {
        log.info("========================================");
        log.info("Shutting down CacheNode...");
        log.info("========================================");

        try {
            if (metricsServer != null) {
                metricsServer.shutdown();
            }

            if (failoverManager != null) {
                log.info("Stopping FailoverManager...");
                failoverManager.stop();
                log.info("[OK] FailoverManager stopped");
            }

            if (netServer != null) {
                log.info("Stopping NetServer...");
                netServer.stop();
                log.info("[OK] NetServer stopped");
            }

            if (replicationManager != null) {
                log.info("Stopping ReplicationManager...");
                replicationManager.shutdown();
                log.info("[OK] ReplicationManager stopped");
            }

            if (replicationSender != null) {
                log.info("Stopping ReplicationSender...");
                replicationSender.shutdown();
                log.info("[OK] ReplicationSender stopped");
            }

            if (replicationReceiver != null) {
                log.info("Stopping ReplicationReceiver...");
                replicationReceiver.stop();
                log.info("[OK] ReplicationReceiver stopped");
            }

            if (cache != null) {
                log.info("Clearing cache...");
                cache.clear();
                log.info("[OK] Cache cleared");
            }

            log.info("========================================");
            log.info("[SUCCESS] Shutdown complete");
            log.info("========================================");
        } catch (Exception e) {
            log.error("Error during shutdown", e);
        } finally {
            shutdownLatch.countDown();
        }
    }

    private void awaitShutdown() {
        try {
            shutdownLatch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}