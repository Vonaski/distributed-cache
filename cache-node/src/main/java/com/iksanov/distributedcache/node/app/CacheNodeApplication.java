package com.iksanov.distributedcache.node.app;

import com.iksanov.distributedcache.node.config.*;
import com.iksanov.distributedcache.node.consensus.sharding.ShardManager;
import com.iksanov.distributedcache.node.core.InMemoryCacheStore;
import com.iksanov.distributedcache.node.metrics.*;
import com.iksanov.distributedcache.node.net.NetServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

public final class CacheNodeApplication {
    private static final Logger log = LoggerFactory.getLogger(CacheNodeApplication.class);
    private final ApplicationConfig appConfig;
    private final CountDownLatch shutdownLatch = new CountDownLatch(1);
    private InMemoryCacheStore cache;
    private MetricsServer metricsServer;
    private NetServer netServer;
    private CacheMetrics cacheMetrics;
    private NetMetrics netMetrics;
    private RaftMetrics raftMetrics;
    private ShardManager shardManager;

    public CacheNodeApplication(ApplicationConfig appConfig) {
        this.appConfig = appConfig;
    }

    public static void main(String[] args) {
        ApplicationConfig cfg = ApplicationConfig.fromEnv();
        CacheNodeApplication app = new CacheNodeApplication(cfg);
        log.info("=== Starting CacheNode {} ===", cfg.nodeId());
        log.info("host={}, clientPort={}", cfg.host(), cfg.port());
        log.info("clusterNodes={}", cfg.clusterNodes());
        app.start();
        app.awaitShutdown();
    }

    public void start() {
        try {
            initMetrics();
            initCache();
            int metricsPort = Integer.parseInt(System.getenv().getOrDefault("METRICS_PORT", "8081"));
            metricsServer = new MetricsServer(metricsPort, cacheMetrics, raftMetrics, netMetrics);
            metricsServer.start();
            log.info("Metrics server started on {}", metricsPort);
            ClusterConfig clusterConfig = ClusterConfig.fromEnv();
            RaftConfig raftConfig = appConfig.productionMode()
                    ? RaftConfig.production()
                    : RaftConfig.defaults();
            int virtualNodes = Integer.parseInt(System.getenv().getOrDefault("CACHE_RING_VIRTUAL_NODES", "128"));
            shardManager = new ShardManager(
                    appConfig.nodeId(),
                    appConfig.host(),
                    cache,
                    raftMetrics,
                    raftConfig,
                    virtualNodes
            );

            shardManager.startShards(clusterConfig);
            log.info("ShardManager initialized, local shards = {}", shardManager.localShardIds());
            NetServerConfig netCfg = new NetServerConfig(
                    appConfig.host(),
                    appConfig.port(),
                    1,  // bossThreads
                    1,  // workerThreads
                    128,  // backlog
                    16 * 1024 * 1024,  // maxFrameLength (16MB)
                    2,  // shutdownQuietPeriodSeconds
                    10,  // shutdownTimeoutSeconds
                    100L,  // slowRequestThresholdMs
                    10L  // consensusTimeoutSeconds
            );

            netServer = new NetServer(netCfg, shardManager, netMetrics);
            netServer.start();
            log.info("NetServer started on {}:{}", appConfig.host(), appConfig.port());
            Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown, "shutdown-hook"));
            log.info("=== CacheNode {} ready ===", appConfig.nodeId());
        } catch (Throwable t) {
            log.error("Startup failed", t);
            shutdown();
            System.exit(1);
        }
    }

    private void initMetrics() {
        cacheMetrics = new CacheMetrics();
        raftMetrics = new RaftMetrics();
        netMetrics = new NetMetrics();
        log.info("Metrics instances created");
    }

    private void initCache() {
        cache = new InMemoryCacheStore(
                appConfig.cacheMaxSize(),
                appConfig.cacheTtlMillis(),
                5000,
                cacheMetrics
        );
        log.info("Cache initialized (maxSize={}, ttlMs={})", appConfig.cacheMaxSize(), appConfig.cacheTtlMillis());
    }

    private void shutdown() {
        log.info("Shutting down CacheNode {}", appConfig.nodeId());
        try {
            if (metricsServer != null) metricsServer.shutdown();
            if (netServer != null) netServer.stop();
            if (shardManager != null) shardManager.stopAll();
            if (cache != null) cache.clear();
            log.info("Shutdown complete");
        } catch (Throwable t) {
            log.error("Error during shutdown", t);
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
