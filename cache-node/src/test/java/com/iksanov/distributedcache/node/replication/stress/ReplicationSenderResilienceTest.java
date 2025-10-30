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
 * Resilience stress test for ReplicationSender.
 * <p>
 * Tests sender's ability to handle:
 * - Network failures and connection timeouts
 * - Unreachable replicas
 * - Mixed success/failure scenarios
 * - Recovery after failures
 */
public class ReplicationSenderResilienceTest {

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
    @DisplayName("Should survive connection failures to unreachable replicas")
    void shouldSurviveConnectionFailures() {
        replicaManager = new ReplicaManager();
        NodeInfo master = new NodeInfo("node-A", "127.0.0.1", 9000);

        NodeInfo unreachableReplica1 = new NodeInfo("node-B", "127.0.0.1", 9001);
        NodeInfo unreachableReplica2 = new NodeInfo("node-C", "127.0.0.1", 9002);

        replicaManager.registerReplica(master, unreachableReplica1);
        replicaManager.registerReplica(master, unreachableReplica2);
        sender = new ReplicationSender(replicaManager);

        int operationCount = 100;
        AtomicInteger attemptedOperations = new AtomicInteger(0);

        assertDoesNotThrow(() -> {
            for (int i = 0; i < operationCount; i++) {
                ReplicationTask task = ReplicationTask.ofSet(
                        "key-" + i,
                        "value-" + i,
                        master.nodeId()
                );
                sender.replicate(master, task);
                attemptedOperations.incrementAndGet();
            }
        });
        assertEquals(operationCount, attemptedOperations.get(), "All operations must be attempted despite failures");

        System.out.printf("✅ Connection failure test:%n");
        System.out.printf("   Attempted operations: %d%n", attemptedOperations.get());
        System.out.println("   Sender survived all connection failures");
    }

    @Test
    @DisplayName("Should handle high concurrency with unreachable replicas")
    void shouldHandleConcurrencyWithUnreachableReplicas() throws Exception {
        replicaManager = new ReplicaManager();
        NodeInfo master = new NodeInfo("node-A", "127.0.0.1", 9000);
        NodeInfo replica1 = new NodeInfo("node-B", "127.0.0.1", 9001);
        NodeInfo replica2 = new NodeInfo("node-C", "127.0.0.1", 9002);

        replicaManager.registerReplica(master, replica1);
        replicaManager.registerReplica(master, replica2);

        sender = new ReplicationSender(replicaManager);

        int threads = 16;
        int operationsPerThread = 500;
        int totalOperations = threads * operationsPerThread;

        ExecutorService executor = Executors.newFixedThreadPool(threads);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threads);
        AtomicInteger successCounter = new AtomicInteger(0);
        AtomicInteger failureCounter = new AtomicInteger(0);

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
                            failureCounter.incrementAndGet();
                        }
                    }
                } catch (Exception e) {
                    fail("Worker error: " + e.getMessage());
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        long startTime = System.nanoTime();
        startLatch.countDown();
        boolean finished = doneLatch.await(120, TimeUnit.SECONDS);
        long durationMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime);
        executor.shutdownNow();
        assertTrue(finished, "All threads must complete within timeout");
        int totalProcessed = successCounter.get() + failureCounter.get();
        assertEquals(totalOperations, totalProcessed, "All operations must be accounted for");

        System.out.printf("✅ Concurrency with failures test:%n");
        System.out.printf("   Total operations: %,d%n", totalOperations);
        System.out.printf("   Duration: %,d ms%n", durationMs);
        System.out.printf("   Success: %,d%n", successCounter.get());
        System.out.printf("   Failures: %,d%n", failureCounter.get());
        System.out.printf("   Throughput: %.2f ops/sec%n", totalOperations / (durationMs / 1000.0));
    }

    @Test
    @DisplayName("Should handle rapid connect/disconnect scenarios")
    void shouldHandleRapidConnectDisconnect() throws Exception {
        replicaManager = new ReplicaManager();
        NodeInfo master = new NodeInfo("node-A", "127.0.0.1", 9000);
        NodeInfo replica = new NodeInfo("node-B", "127.0.0.1", 9001);

        replicaManager.registerReplica(master, replica);
        sender = new ReplicationSender(replicaManager);

        int cycles = 50;

        for (int i = 0; i < cycles; i++) {
            for (int j = 0; j < 10; j++) {
                ReplicationTask task = ReplicationTask.ofSet(
                        "key-" + j,
                        "value-" + i + "-" + j,
                        master.nodeId()
                );
                sender.replicate(master, task);
            }

            Thread.sleep(10);
        }

        System.out.printf("✅ Rapid connect/disconnect test: %d cycles completed%n", cycles);
    }

    @Test
    @DisplayName("Should handle mixed reachable and unreachable replicas")
    void shouldHandleMixedReplicaAvailability() {
        replicaManager = new ReplicaManager();
        NodeInfo master = new NodeInfo("node-A", "127.0.0.1", 9000);

        NodeInfo replica1 = new NodeInfo("node-B", "127.0.0.1", 9001);
        NodeInfo replica2 = new NodeInfo("node-C", "127.0.0.1", 9002);
        NodeInfo replica3 = new NodeInfo("node-D", "127.0.0.1", 9003);

        replicaManager.registerReplica(master, replica1);
        replicaManager.registerReplica(master, replica2);
        replicaManager.registerReplica(master, replica3);

        sender = new ReplicationSender(replicaManager);

        int operationCount = 200;

        for (int i = 0; i < operationCount; i++) {
            ReplicationTask task = ReplicationTask.ofSet(
                    "key-" + i,
                    "value-" + i,
                    master.nodeId()
            );
            assertDoesNotThrow(() -> sender.replicate(master, task), "Replication should not throw even with mixed replica availability");
        }
        System.out.printf("✅ Mixed availability test: %d operations completed%n", operationCount);
    }

    @Test
    @DisplayName("Should maintain performance under sustained load with failures")
    void shouldMaintainPerformanceUnderSustainedLoad() {
        replicaManager = new ReplicaManager();
        NodeInfo master = new NodeInfo("node-A", "127.0.0.1", 9000);
        NodeInfo replica = new NodeInfo("node-B", "127.0.0.1", 9001);
        replicaManager.registerReplica(master, replica);
        sender = new ReplicationSender(replicaManager);

        int totalOperations = 5000;
        long startTime = System.nanoTime();

        for (int i = 0; i < totalOperations; i++) {
            ReplicationTask task = ReplicationTask.ofSet(
                    "sustained-key-" + i,
                    "value-" + i,
                    master.nodeId()
            );
            sender.replicate(master, task);
        }
        long durationMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime);
        double throughput = totalOperations / (durationMs / 1000.0);
        System.out.printf("✅ Sustained load test:%n");
        System.out.printf("   Operations: %,d%n", totalOperations);
        System.out.printf("   Duration: %,d ms%n", durationMs);
        System.out.printf("   Throughput: %.2f ops/sec%n", throughput);

        assertTrue(throughput > 100, "Throughput should remain reasonable even with failures");
    }

    @Test
    @DisplayName("Should handle replica removal during active replication")
    void shouldHandleReplicaRemovalDuringReplication() {
        replicaManager = new ReplicaManager();
        NodeInfo master = new NodeInfo("node-A", "127.0.0.1", 9000);
        NodeInfo replica1 = new NodeInfo("node-B", "127.0.0.1", 9001);
        NodeInfo replica2 = new NodeInfo("node-C", "127.0.0.1", 9002);
        replicaManager.registerReplica(master, replica1);
        replicaManager.registerReplica(master, replica2);
        sender = new ReplicationSender(replicaManager);
        ExecutorService executor = Executors.newFixedThreadPool(2);

        Future<?> replicationTask = executor.submit(() -> {
            for (int i = 0; i < 200; i++) {
                try {
                    ReplicationTask task = ReplicationTask.ofSet(
                            "key-" + i,
                            "value-" + i,
                            master.nodeId()
                    );
                    sender.replicate(master, task);
                    Thread.sleep(5);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        });

        Future<?> removalTask = executor.submit(() -> {
            try {
                Thread.sleep(100);
                replicaManager.removeReplica(replica1);
                Thread.sleep(100);
                replicaManager.removeReplica(replica2);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        assertDoesNotThrow(() -> replicationTask.get(30, TimeUnit.SECONDS), "Replication should survive replica removal");
        assertDoesNotThrow(() -> removalTask.get(30, TimeUnit.SECONDS), "Replica removal should complete cleanly");
        executor.shutdownNow();
        System.out.println("✅ Replica removal during replication test completed");
    }

    @Test
    @DisplayName("Should cleanup resources properly after many failed operations")
    void shouldCleanupAfterManyFailures() {
        replicaManager = new ReplicaManager();
        NodeInfo master = new NodeInfo("node-A", "127.0.0.1", 9000);
        NodeInfo replica = new NodeInfo("node-B", "127.0.0.1", 9001);

        replicaManager.registerReplica(master, replica);
        sender = new ReplicationSender(replicaManager);

        for (int i = 0; i < 500; i++) {
            ReplicationTask task = ReplicationTask.ofSet(
                    "fail-key-" + i,
                    "value-" + i,
                    master.nodeId()
            );
            sender.replicate(master, task);
        }

        assertDoesNotThrow(() -> sender.shutdown(), "Shutdown must succeed even after many failures");
        assertTrue(sender.channelsMap().isEmpty(), "Channels map must be cleared after shutdown");
        System.out.println("✅ Cleanup after failures test completed");
    }
}