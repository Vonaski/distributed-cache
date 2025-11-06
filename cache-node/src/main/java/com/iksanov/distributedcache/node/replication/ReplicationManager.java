package com.iksanov.distributedcache.node.replication;

import com.iksanov.distributedcache.common.cluster.NodeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * Manages asynchronous replication of cache operations across distributed nodes.
 * <p>
 * This component coordinates between master and replica nodes, ensuring data consistency
 * through async replication of SET and DELETE operations.
 * <p>
 * Design principles:
 * <ul>
 *   <li>Non-blocking replication to maintain low latency on primary operations</li>
 *   <li>Single-threaded executor ensures FIFO ordering for replication tasks</li>
 *   <li>Graceful error handling prevents replication failures from affecting primary operations</li>
 *   <li>Clean shutdown with configurable timeout for pending tasks</li>
 * </ul>
 * <p>
 * Thread safety: This class is thread-safe and can be safely accessed by multiple threads.
 *
 * @see ReplicationSender
 * @see ReplicationReceiver
 * @see ReplicationTask
 */
public class ReplicationManager {

    private static final Logger log = LoggerFactory.getLogger(ReplicationManager.class);
    private static final int SHUTDOWN_TIMEOUT_SECONDS = 5;
    private static final AtomicInteger INSTANCE_COUNTER = new AtomicInteger(0);
    private final NodeInfo currentNode;
    private final ReplicationSender sender;
    private final ReplicationReceiver receiver;
    private final Function<String, NodeInfo> primaryResolver;
    private final ExecutorService replicationExecutor;
    private final int instanceId;
    private final AtomicLong sequenceGenerator = new AtomicLong(0);

    public ReplicationManager(NodeInfo currentNode, ReplicationSender sender, ReplicationReceiver receiver, Function<String, NodeInfo> primaryResolver) {
        this.currentNode = Objects.requireNonNull(currentNode, "currentNode cannot be null");
        this.sender = Objects.requireNonNull(sender, "sender cannot be null");
        this.receiver = Objects.requireNonNull(receiver, "receiver cannot be null");
        this.primaryResolver = Objects.requireNonNull(primaryResolver, "primaryResolver cannot be null");
        this.instanceId = INSTANCE_COUNTER.incrementAndGet();
        this.replicationExecutor = createReplicationExecutor(currentNode.nodeId(), instanceId);
        log.info("ReplicationManager initialized for node: {} (instance: {})", currentNode.nodeId(), instanceId);
    }

    private static ExecutorService createReplicationExecutor(String nodeId, int instanceId) {
        ThreadFactory factory = runnable -> {
            Thread thread = new Thread(runnable);
            thread.setName(String.format("replication-%s-%d", nodeId, instanceId));
            thread.setDaemon(true);
            thread.setUncaughtExceptionHandler((t, e) ->
                    log.error("Uncaught exception in replication thread {}: {}", t.getName(), e.getMessage(), e)
            );
            return thread;
        };
        return Executors.newSingleThreadExecutor(factory);
    }

    public void onLocalSet(String key, String value) {
        Objects.requireNonNull(key, "key cannot be null");
        Objects.requireNonNull(value, "value cannot be null");
        if (!isMasterForKey(key)) return;
        long seq = sequenceGenerator.incrementAndGet();
        ReplicationTask task = ReplicationTask.ofSet(key, value, currentNode.nodeId(), seq);
        submitReplicationTask(task, "SET");
    }

    public void onLocalDelete(String key) {
        Objects.requireNonNull(key, "key cannot be null");
        if (!isMasterForKey(key)) return;
        long seq = sequenceGenerator.incrementAndGet();
        ReplicationTask task = ReplicationTask.ofDelete(key, currentNode.nodeId(), seq);
        submitReplicationTask(task, "DELETE");
    }

    private boolean isMasterForKey(String key) {
        try {
            NodeInfo master = primaryResolver.apply(key);
            if (master == null) {
                log.warn("Primary resolver returned null for key={}, skipping replication", key);
                return false;
            }
            boolean isMaster = currentNode.equals(master);
            if (!isMaster) log.trace("Node {} is not master for key={} (master={})", currentNode.nodeId(), key, master.nodeId());
            return isMaster;
        } catch (Exception e) {
            log.error("Error determining master for key={}: {}", key, e.getMessage(), e);
            return false;
        }
    }

    private void submitReplicationTask(ReplicationTask task, String operationType) {
        replicationExecutor.submit(() -> {
            try {
                long startTime = System.nanoTime();
                sender.replicate(currentNode, task);
                long durationMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime);

                if (durationMs > 100) {
                    log.warn("Slow replication of {} for key={} took {}ms", operationType, task.key(), durationMs);
                } else {
                    log.debug("Replicated {} for key={} in {}ms", operationType, task.key(), durationMs);
                }
            } catch (Exception e) {
                log.error("Failed to replicate {} for key={}: {}", operationType, task.key(), e.getMessage(), e);
            }
        });
    }

    public void onReplicationReceived(ReplicationTask task) {
        if (task == null) {
            log.warn("Received null replication task, ignoring");
            return;
        }

        try {
            log.debug("Applying replication: operation={}, key={}, origin={}", task.operation(), task.key(), task.origin());
            receiver.applyTask(task);
            log.trace("Successfully applied replication for key={}", task.key());
        } catch (Exception e) {
            log.error("Failed to apply replication task for key={}: {}", task.key(), e.getMessage(), e);
        }
    }

    public void shutdown() {
        log.info("Starting shutdown of ReplicationManager for node: {}", currentNode.nodeId());
        replicationExecutor.shutdown();
        try {
            if (!replicationExecutor.awaitTermination(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                log.warn("Replication executor did not terminate within {}s, forcing shutdown", SHUTDOWN_TIMEOUT_SECONDS);
                replicationExecutor.shutdownNow();

                if (!replicationExecutor.awaitTermination(2, TimeUnit.SECONDS)) {
                    log.error("Replication executor did not terminate after forced shutdown");
                }
            }
        } catch (InterruptedException e) {
            log.warn("Interrupted while waiting for replication executor shutdown");
            replicationExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }

        try {
            sender.shutdown();
        } catch (Exception e) {
            log.error("Error while shutting down replication sender: {}", e.getMessage(), e);
        }
        log.info("ReplicationManager shutdown completed for node: {}", currentNode.nodeId());
    }

    public NodeInfo getCurrentNode() {
        return currentNode;
    }

    public boolean isShutdown() {
        return replicationExecutor.isShutdown();
    }

    public boolean isTerminated() {
        return replicationExecutor.isTerminated();
    }
}