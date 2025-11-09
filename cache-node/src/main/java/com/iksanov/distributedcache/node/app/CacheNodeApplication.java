package com.iksanov.distributedcache.node.app;

import com.iksanov.distributedcache.common.cluster.NodeInfo;
import com.iksanov.distributedcache.common.cluster.ReplicaManager;
import com.iksanov.distributedcache.common.cluster.sharding.ConsistentHashRing;
import com.iksanov.distributedcache.node.config.ApplicationConfig;
import com.iksanov.distributedcache.node.config.ApplicationConfig.NodeRole;
import com.iksanov.distributedcache.node.core.InMemoryCacheStore;
import com.iksanov.distributedcache.node.metrics.CacheMetrics;
import com.iksanov.distributedcache.node.metrics.MetricsServer;
import com.iksanov.distributedcache.node.metrics.NetMetrics;
import com.iksanov.distributedcache.node.metrics.RaftMetrics;
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
    private final CountDownLatch shutdownLatch = new CountDownLatch(1);
    private CacheMetrics cacheMetrics;
    private RaftMetrics raftMetrics;
    private MetricsServer metricsServer;
    private NetMetrics netMetrics;

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
            raftMetrics = new RaftMetrics();
            netMetrics = new NetMetrics();
            log.info("✓ Metrics initialized");

            cache = new InMemoryCacheStore(config.cacheMaxSize(), config.cacheTtlMillis(), 5000, cacheMetrics);
            log.info("✓ Cache initialized (size={})", config.cacheMaxSize());

            metricsServer = new MetricsServer(8081, cacheMetrics, raftMetrics, netMetrics);
            metricsServer.start();
            log.info("✓ Metrics server started on port 8081");

            if (config.clusterEnabled()) {
                replicationManager = initCluster();
                log.info("✓ Cluster mode enabled (role={})", config.role());
            }

            netServer = new NetServer(config.toNetServerConfig(), cache, replicationManager, netMetrics);
            netServer.start();
            log.info("✓ Server listening on {}:{}", config.host(), config.port());

            Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown, "shutdown-hook"));

            log.info("========================================");
            log.info("✅ CacheNode ready!");
            log.info("   Cache port: {}", config.port());
            log.info("   Metrics port: 8081");
            log.info("========================================");

        } catch (Exception e) {
            log.error("Startup failed", e);
            shutdown();
            System.exit(1);
        }
    }

    private ReplicationManager initCluster() {
        NodeInfo self = new NodeInfo(config.nodeId(), config.host(), config.port(), config.replicationPort());

        ConsistentHashRing hashRing = new ConsistentHashRing(150);
        ReplicaManager replicaManager = new ReplicaManager();

        if (config.role() == NodeRole.MASTER) {
            log.info("Initializing as MASTER node...");
            hashRing.addNode(self);
            log.info("  Added self to hash ring: {}", self.nodeId());
            for (String nodeAddr : config.clusterNodes()) {
                try {
                    NodeInfo node = NodeInfo.fromString(nodeAddr);
                    if (node.nodeId().contains("replica")) {
                        replicaManager.registerReplica(self, node);
                        log.info("  Registered replica: {}", node.nodeId());
                    } else if (node.nodeId().contains("master")) {
                        hashRing.addNode(node);
                        log.info("  Added master to hash ring: {}", node.nodeId());
                    } else {
                        hashRing.addNode(node);
                        log.info("  Added node to hash ring: {}", node.nodeId());
                    }
                } catch (Exception e) {
                    log.warn("Failed to parse node '{}': {}", nodeAddr, e.getMessage());
                }
            }

            log.info("Hash ring size: {}", hashRing.getNodes().size());
            log.info("Replicas registered: {}", replicaManager.getReplicas(self).size());
        } else {
            log.info("Initializing as REPLICA node...");
            if (!config.clusterNodes().isEmpty()) {
                try {
                    NodeInfo master = NodeInfo.fromString(config.clusterNodes().getFirst());
                    hashRing.addNode(master);
                    log.info("  Connected to master: {}", master.nodeId());
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
                config.nodeId()
        );

        replicationReceiver.start();
        log.info("✓ ReplicationReceiver started on port {}", config.replicationPort());
        replicationSender = new ReplicationSender(replicaManager);
        log.info("✓ ReplicationSender initialized");

        return new ReplicationManager(
                self,
                replicationSender,
                replicationReceiver,
                key -> {
                    if (hashRing.getNodeForKey(key) != null) {
                        return hashRing.getNodeForKey(key);
                    }
                    return self;
                }
        );
    }

    private void shutdown() {
        log.info("========================================");
        log.info("Shutting down CacheNode...");
        log.info("========================================");

        try {
            if (metricsServer != null) {
                metricsServer.shutdown();
            }

            if (netServer != null) {
                log.info("Stopping NetServer...");
                netServer.stop();
                log.info("✓ NetServer stopped");
            }

            if (replicationManager != null) {
                log.info("Stopping ReplicationManager...");
                replicationManager.shutdown();
                log.info("✓ ReplicationManager stopped");
            }

            if (replicationSender != null) {
                log.info("Stopping ReplicationSender...");
                replicationSender.shutdown();
                log.info("✓ ReplicationSender stopped");
            }

            if (replicationReceiver != null) {
                log.info("Stopping ReplicationReceiver...");
                replicationReceiver.stop();
                log.info("✓ ReplicationReceiver stopped");
            }

            if (cache != null) {
                log.info("Clearing cache...");
                cache.clear();
                log.info("✓ Cache cleared");
            }

            log.info("========================================");
            log.info("✅ Shutdown complete");
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