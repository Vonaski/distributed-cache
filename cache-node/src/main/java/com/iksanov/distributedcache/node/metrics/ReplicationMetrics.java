package com.iksanov.distributedcache.node.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Timer;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Metrics collector for replication subsystem using Micrometer.
 * Exposes replication statistics to Prometheus and Grafana.
 */
public class ReplicationMetrics {
    private static final Logger log = LoggerFactory.getLogger(ReplicationMetrics.class);
    private final PrometheusMeterRegistry registry;
    private final Counter tasksSubmitted;
    private final Counter tasksSucceeded;
    private final Counter tasksFailed;
    private final Counter bytesReplicated;
    private final Counter keysReplicated;
    private final Counter receiverApplied;
    private final Counter receiverFailed;
    private final Timer replicationLatency;
    private final AtomicInteger activeSender = new AtomicInteger(0);
    private final AtomicInteger activeReceiver = new AtomicInteger(0);
    private final AtomicLong pendingTasks = new AtomicLong(0);

    public ReplicationMetrics() {
        this.registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);

        this.tasksSubmitted = Counter.builder("replication_tasks_submitted_total")
                .description("Total number of replication tasks submitted for sending")
                .register(registry);

        this.tasksSucceeded = Counter.builder("replication_tasks_succeeded_total")
                .description("Total number of replication tasks successfully acknowledged by replicas")
                .register(registry);

        this.tasksFailed = Counter.builder("replication_tasks_failed_total")
                .description("Total number of replication tasks that failed to send or apply")
                .register(registry);

        this.bytesReplicated = Counter.builder("replication_bytes_total")
                .description("Total number of bytes replicated to other nodes")
                .baseUnit("bytes")
                .register(registry);

        this.keysReplicated = Counter.builder("replication_keys_total")
                .description("Total number of keys replicated to other nodes")
                .register(registry);

        this.receiverApplied = Counter.builder("replication_receiver_applied_total")
                .description("Total number of replication entries successfully applied by follower nodes")
                .register(registry);

        this.receiverFailed = Counter.builder("replication_receiver_failed_total")
                .description("Total number of replication tasks failed on receiver side")
                .register(registry);

        this.replicationLatency = Timer.builder("replication_latency_seconds")
                .description("Time taken for replication task from send to apply")
                .register(registry);

        Gauge.builder("replication_sender_active", activeSender, AtomicInteger::get)
                .description("Indicates whether replication sender is active (1) or passive (0)")
                .register(registry);

        Gauge.builder("replication_receiver_active", activeReceiver, AtomicInteger::get)
                .description("Indicates whether replication receiver is active (1) or passive (0)")
                .register(registry);

        Gauge.builder("replication_tasks_pending", pendingTasks, AtomicLong::get)
                .description("Current number of replication tasks pending in queue")
                .register(registry);

        log.info("✅ ReplicationMetrics initialized and Prometheus registry created");
    }

    public void incrementSubmitted() { tasksSubmitted.increment(); }
    public void incrementSucceeded() { tasksSucceeded.increment(); }
    public void incrementFailed() { tasksFailed.increment(); }
    public void incrementBytes(long bytes) { bytesReplicated.increment(bytes); }
    public void incrementKeys() { keysReplicated.increment(); }
    public void incrementReceiverApplied() { receiverApplied.increment(); }
    public void incrementReceiverFailed() { receiverFailed.increment(); }
    public Timer.Sample startReplicationTimer() { return Timer.start(registry); }
    public void stopReplicationTimer(Timer.Sample sample) { sample.stop(replicationLatency); }
    public void setSenderActive(boolean active) { activeSender.set(active ? 1 : 0); }
    public void setReceiverActive(boolean active) { activeReceiver.set(active ? 1 : 0); }
    public void updatePendingTasks(long count) { pendingTasks.set(count); }
    public String scrape() { return registry.scrape(); }
    public PrometheusMeterRegistry getRegistry() { return registry; }
}
