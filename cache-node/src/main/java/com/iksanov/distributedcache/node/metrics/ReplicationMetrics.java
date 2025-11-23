package com.iksanov.distributedcache.node.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Timer;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Metrics collector for asynchronous replication.
 * Exposes replication statistics to Prometheus for monitoring data consistency
 * and replication performance across distributed cache nodes.
 */
public class ReplicationMetrics {

    private static final Logger log = LoggerFactory.getLogger(ReplicationMetrics.class);
    private final PrometheusMeterRegistry registry;

    private final Counter replicationsSent;
    private final Counter replicationsReceived;
    private final Counter replicationsFailed;
    private final Counter replicationsIgnored;
    private final Counter connectionsEstablished;
    private final Counter connectionsFailed;
    private final Timer replicationDuration;
    private final AtomicLong activeReplicationConnections = new AtomicLong(0);
    private final AtomicLong pendingReplicationTasks = new AtomicLong(0);

    public ReplicationMetrics() {
        this.registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);

        this.replicationsSent = Counter.builder("replication.sent.total")
                .description("Total number of replication tasks sent to replicas")
                .register(registry);

        this.replicationsReceived = Counter.builder("replication.received.total")
                .description("Total number of replication tasks received from master")
                .register(registry);

        this.replicationsFailed = Counter.builder("replication.failed.total")
                .description("Total number of failed replication attempts")
                .register(registry);

        this.replicationsIgnored = Counter.builder("replication.ignored.total")
                .description("Total number of ignored replication tasks (outdated or self-origin)")
                .register(registry);

        this.connectionsEstablished = Counter.builder("replication.connections.established")
                .description("Total number of replication connections established")
                .register(registry);

        this.connectionsFailed = Counter.builder("replication.connections.failed")
                .description("Total number of failed replication connection attempts")
                .register(registry);

        this.replicationDuration = Timer.builder("replication.duration")
                .description("Time taken to replicate a task to all replicas")
                .register(registry);

        Gauge.builder("replication.connections.active", activeReplicationConnections, AtomicLong::get)
                .description("Current number of active replication connections")
                .register(registry);

        Gauge.builder("replication.tasks.pending", pendingReplicationTasks, AtomicLong::get)
                .description("Current number of pending replication tasks in queue")
                .register(registry);

        log.info("[SUCCESS] ReplicationMetrics initialized and Prometheus registry created");
    }

    public void incrementReplicationsSent() {
        replicationsSent.increment();
    }

    public void incrementReplicationsReceived() {
        replicationsReceived.increment();
    }

    public void incrementReplicationsFailed() {
        replicationsFailed.increment();
    }

    public void incrementReplicationsIgnored() {
        replicationsIgnored.increment();
    }

    public void incrementConnectionsEstablished() {
        connectionsEstablished.increment();
        activeReplicationConnections.incrementAndGet();
    }

    public void incrementConnectionsFailed() {
        connectionsFailed.increment();
    }

    public void decrementActiveConnections() {
        activeReplicationConnections.decrementAndGet();
    }

    public void incrementPendingTasks() {
        pendingReplicationTasks.incrementAndGet();
    }

    public void decrementPendingTasks() {
        pendingReplicationTasks.decrementAndGet();
    }

    public Timer.Sample startReplicationTimer() {
        return Timer.start(registry);
    }

    public void stopReplicationTimer(Timer.Sample sample) {
        sample.stop(replicationDuration);
    }

    public String scrape() {
        return registry.scrape();
    }

    public PrometheusMeterRegistry getRegistry() {
        return registry;
    }
}
