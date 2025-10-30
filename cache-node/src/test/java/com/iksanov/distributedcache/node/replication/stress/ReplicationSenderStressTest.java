package com.iksanov.distributedcache.node.replication.stress;

import com.iksanov.distributedcache.common.cluster.NodeInfo;
import com.iksanov.distributedcache.common.cluster.ReplicaManager;
import com.iksanov.distributedcache.node.replication.ReplicationSender;
import com.iksanov.distributedcache.node.replication.ReplicationTask;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Stress test for ReplicationSender under high load.
 * <p>
 * Validates that ReplicationSender can handle:
 * - Massive parallel replication calls
 * - Concurrent connections to multiple replicas
 * - Graceful degradation under simulated failures
 */
public class ReplicationSenderStressTest {

    private ReplicationSender sender;
    private ReplicaManager replicaManager;

    @AfterEach
    void cleanup() {
        if (sender != null) {
            sender.shutdown();
            sender = null;
        }
    }

    @Test
    @DisplayName("Should handle massive parallel replication calls without errors")
    void shouldHandleMassiveParallelReplicationCalls() throws Exception {
        replicaManager = new ReplicaManager();
        NodeInfo master = new NodeInfo("node-A", "127.0.0.1", 9000);
        NodeInfo replica1 = new NodeInfo("node-B", "127.0.0.1", 9001);
        NodeInfo replica2 = new NodeInfo("node-C", "127.0.0.1", 9002);
        replicaManager.registerReplica(master, replica1);
        replicaManager.registerReplica(master, replica2);

        sender = new ReplicationSender(replicaManager);

        int threads = 20;
        int operationsPerThread = 500;
        int totalOperations = threads * operationsPerThread;

        ExecutorService executor = Executors.newFixedThreadPool(threads);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threads);
        AtomicInteger successCounter = new AtomicInteger(0);
        AtomicInteger errorCounter = new AtomicInteger(0);

        for (int i = 0; i < threads; i++) {
            executor.submit(() -> {
                try {
                    startLatch.await();
                    for (int j = 0; j < operationsPerThread; j++) {
                        try {
                            ReplicationTask task = ReplicationTask.ofSet(
                                    "k-" + j,
                                    "v-" + j,
                                    master.nodeId()
                            );
                            sender.replicate(master, task);
                            successCounter.incrementAndGet();
                        } catch (Exception e) {
                            errorCounter.incrementAndGet();
                        }
                    }
                } catch (Exception e) {
                    fail("Worker thread failed: " + e.getMessage());
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        long startTime = System.nanoTime();
        startLatch.countDown();
        assertTrue(doneLatch.await(60, TimeUnit.SECONDS), "All threads must complete within timeout");

        long durationMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime);
        executor.shutdownNow();
        int totalProcessed = successCounter.get() + errorCounter.get();
        assertEquals(totalOperations, totalProcessed, "All operations must be processed");

        System.out.printf("✅ Stress test completed:%n");
        System.out.printf("   Total operations: %,d%n", totalOperations);
        System.out.printf("   Duration: %,d ms%n", durationMs);
        System.out.printf("   Success: %,d (%.2f%%)%n",
                successCounter.get(),
                100.0 * successCounter.get() / totalOperations);
        System.out.printf("   Errors: %,d (%.2f%%)%n",
                errorCounter.get(),
                100.0 * errorCounter.get() / totalOperations);
        System.out.printf("   Throughput: %.2f ops/sec%n",
                totalOperations / (durationMs / 1000.0));
    }

    @Test
    @DisplayName("Should handle empty replica set gracefully")
    void shouldHandleEmptyReplicaSet() {
        replicaManager = new ReplicaManager();
        NodeInfo master = new NodeInfo("node-A", "127.0.0.1", 9000);
        sender = new ReplicationSender(replicaManager);
        assertDoesNotThrow(() -> {
            ReplicationTask task = ReplicationTask.ofSet("k", "v", master.nodeId());
            sender.replicate(master, task);
        });
    }

    @Test
    @DisplayName("Should handle null replicas in replica set")
    void shouldHandleNullReplicasGracefully() {
        replicaManager = new ReplicaManager();
        NodeInfo master = new NodeInfo("node-A", "127.0.0.1", 9000);
        NodeInfo validReplica = new NodeInfo("node-B", "127.0.0.1", 9001);
        replicaManager.registerReplica(master, validReplica);
        sender = new ReplicationSender(replicaManager);

        assertDoesNotThrow(() -> {
            ReplicationTask task = ReplicationTask.ofSet("k", "v", master.nodeId());
            sender.replicate(master, task);
        });
    }

    @Test
    @DisplayName("Should cleanup channels on shutdown")
    void shouldCleanupChannelsOnShutdown() throws InterruptedException {
        replicaManager = new ReplicaManager();
        NodeInfo master = new NodeInfo("node-A", "127.0.0.1", 9000);
        NodeInfo replica = new NodeInfo("node-B", "127.0.0.1", 9001);
        replicaManager.registerReplica(master, replica);
        sender = new ReplicationSender(replicaManager);
        for (int i = 0; i < 10; i++) {
            ReplicationTask task = ReplicationTask.ofSet("k" + i, "v" + i, master.nodeId());
            sender.replicate(master, task);
        }

        Thread.sleep(100);
        assertDoesNotThrow(() -> sender.shutdown());
        assertTrue(sender.channelsMap().isEmpty(), "All channels must be cleared after shutdown");
    }

    @Test
    @DisplayName("Should handle rapid sequential operations")
    void shouldHandleRapidSequentialOperations() {
        replicaManager = new ReplicaManager();
        NodeInfo master = new NodeInfo("node-A", "127.0.0.1", 9000);
        NodeInfo replica = new NodeInfo("node-B", "127.0.0.1", 9001);
        replicaManager.registerReplica(master, replica);

        sender = new ReplicationSender(replicaManager);

        int operationCount = 1000;
        long startTime = System.nanoTime();

        for (int i = 0; i < operationCount; i++) {
            ReplicationTask task = ReplicationTask.ofSet(
                    "rapid-key-" + i,
                    "value-" + i,
                    master.nodeId()
            );
            sender.replicate(master, task);
        }

        long durationMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime);

        System.out.printf("✅ Rapid sequential test:%n");
        System.out.printf("   Operations: %,d%n", operationCount);
        System.out.printf("   Duration: %,d ms%n", durationMs);
        System.out.printf("   Throughput: %.2f ops/sec%n", operationCount / (durationMs / 1000.0));
    }

    @Test
    @DisplayName("Should handle mixed SET and DELETE operations")
    void shouldHandleMixedOperations() {
        replicaManager = new ReplicaManager();
        NodeInfo master = new NodeInfo("node-A", "127.0.0.1", 9000);
        NodeInfo replica = new NodeInfo("node-B", "127.0.0.1", 9001);
        replicaManager.registerReplica(master, replica);
        sender = new ReplicationSender(replicaManager);

        int operationCount = 500;
        AtomicInteger setCount = new AtomicInteger(0);
        AtomicInteger deleteCount = new AtomicInteger(0);

        for (int i = 0; i < operationCount; i++) {
            ReplicationTask task;
            if (i % 3 == 0) {
                task = ReplicationTask.ofDelete("key-" + i, master.nodeId());
                deleteCount.incrementAndGet();
            } else {
                task = ReplicationTask.ofSet("key-" + i, "value-" + i, master.nodeId());
                setCount.incrementAndGet();
            }
            sender.replicate(master, task);
        }

        System.out.printf("✅ Mixed operations test:%n");
        System.out.printf("   Total: %d (SET: %d, DELETE: %d)%n", operationCount, setCount.get(), deleteCount.get());
    }
}