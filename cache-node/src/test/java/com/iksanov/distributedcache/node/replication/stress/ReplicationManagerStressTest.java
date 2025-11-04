package com.iksanov.distributedcache.node.replication.stress;

import com.iksanov.distributedcache.common.cluster.NodeInfo;
import com.iksanov.distributedcache.node.metrics.ReplicationMetrics;
import com.iksanov.distributedcache.node.replication.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Stress test for {@link ReplicationManager}.
 * <p>
 * Validates that ReplicationManager can handle:
 * - Concurrent local operations (onLocalSet/onLocalDelete)
 * - Concurrent incoming replication (onReplicationReceived)
 * - Mixed workload scenarios
 * - Graceful shutdown under load
 */
public class ReplicationManagerStressTest {

    private ReplicationManager manager;
    private ReplicationSender sender;
    private ReplicationReceiver receiver;
    private NodeInfo self;
    private Function<String, NodeInfo> primaryResolver;
    private ReplicationMetrics replicationMetrics;

    @BeforeEach
    void setup() {
        self = new NodeInfo("node-A", "127.0.0.1", 9000);
        sender = mock(ReplicationSender.class);
        receiver = mock(ReplicationReceiver.class);
        primaryResolver = key -> self;
        replicationMetrics = mock(ReplicationMetrics.class);
        manager = new ReplicationManager(self, sender, receiver, primaryResolver);
    }

    @Test
    @DisplayName("Should handle massive concurrent local SET and DELETE operations")
    void shouldHandleConcurrentLocalOperations() throws InterruptedException {
        int threads = 16;
        int opsPerThread = 2000;
        int totalOps = threads * opsPerThread;

        ExecutorService executor = Executors.newFixedThreadPool(threads);
        CountDownLatch latch = new CountDownLatch(threads);
        AtomicInteger setCounter = new AtomicInteger(0);
        AtomicInteger deleteCounter = new AtomicInteger(0);

        for (int i = 0; i < threads; i++) {
            executor.submit(() -> {
                try {
                    for (int j = 0; j < opsPerThread; j++) {
                        String key = "key-" + j;
                        String value = "v-" + j;
                        if (j % 3 == 0) {
                            manager.onLocalDelete(key);
                            deleteCounter.incrementAndGet();
                        } else {
                            manager.onLocalSet(key, value);
                            setCounter.incrementAndGet();
                        }
                    }
                } catch (Exception e) {
                    fail("Unexpected exception: " + e.getMessage());
                } finally {
                    latch.countDown();
                }
            });
        }
        assertTrue(latch.await(60, TimeUnit.SECONDS), "All worker threads must complete within timeout");
        executor.shutdownNow();
        verify(sender, atLeastOnce()).replicate(eq(self), any(ReplicationTask.class));
        int totalProcessed = setCounter.get() + deleteCounter.get();
        assertEquals(totalOps, totalProcessed, "All operations must be processed");

        System.out.printf("✅ Concurrent local operations test:%n");
        System.out.printf("   Total operations: %,d%n", totalOps);
        System.out.printf("   SET operations: %,d%n", setCounter.get());
        System.out.printf("   DELETE operations: %,d%n", deleteCounter.get());
    }

    @Test
    @DisplayName("Should handle massive concurrent incoming replication")
    void shouldHandleConcurrentIncomingReplication() throws InterruptedException {
        int threads = 12;
        int tasksPerThread = 1500;
        int totalTasks = threads * tasksPerThread;

        ExecutorService executor = Executors.newFixedThreadPool(threads);
        CountDownLatch latch = new CountDownLatch(threads);
        AtomicInteger receivedCounter = new AtomicInteger(0);

        for (int i = 0; i < threads; i++) {
            executor.submit(() -> {
                try {
                    for (int j = 0; j < tasksPerThread; j++) {
                        ReplicationTask task = ReplicationTask.ofSet(
                                "k-" + j,
                                "val-" + j,
                                "node-B"
                        );
                        manager.onReplicationReceived(task);
                        receivedCounter.incrementAndGet();
                    }
                } catch (Exception e) {
                    fail("Unexpected exception: " + e.getMessage());
                } finally {
                    latch.countDown();
                }
            });
        }

        assertTrue(latch.await(60, TimeUnit.SECONDS), "All worker threads must complete within timeout");
        executor.shutdownNow();
        verify(receiver, atLeastOnce()).applyTask(any(ReplicationTask.class));
        assertEquals(totalTasks, receivedCounter.get(), "All incoming replication tasks must be processed");

        System.out.printf("✅ Incoming replication test:%n");
        System.out.printf("   Tasks processed: %,d%n", receivedCounter.get());
    }

    @Test
    @DisplayName("Should handle mixed concurrent load (local + incoming)")
    void shouldHandleMixedConcurrentLoad() throws InterruptedException {
        int threads = 20;
        int opsPerThread = 1000;
        int totalOps = threads * opsPerThread;

        ExecutorService executor = Executors.newFixedThreadPool(threads);
        CountDownLatch latch = new CountDownLatch(threads);
        AtomicInteger localOpsCounter = new AtomicInteger(0);
        AtomicInteger incomingOpsCounter = new AtomicInteger(0);

        for (int i = 0; i < threads; i++) {
            executor.submit(() -> {
                try {
                    for (int j = 0; j < opsPerThread; j++) {
                        String key = "k-" + Thread.currentThread().getId() + "-" + j;
                        if (j % 4 == 0) {
                            ReplicationTask incoming = ReplicationTask.ofSet(
                                    key,
                                    "v-" + j,
                                    "node-B"
                            );
                            manager.onReplicationReceived(incoming);
                            incomingOpsCounter.incrementAndGet();
                        } else if (j % 5 == 0) {
                            manager.onLocalDelete(key);
                            localOpsCounter.incrementAndGet();
                        } else {
                            manager.onLocalSet(key, "v-" + j);
                            localOpsCounter.incrementAndGet();
                        }
                    }
                } catch (Exception e) {
                    fail("Unexpected exception: " + e.getMessage());
                } finally {
                    latch.countDown();
                }
            });
        }

        boolean done = latch.await(90, TimeUnit.SECONDS);
        executor.shutdownNow();
        assertTrue(done, "All worker threads must complete within timeout");
        verify(sender, atLeastOnce()).replicate(eq(self), any(ReplicationTask.class));
        verify(receiver, atLeastOnce()).applyTask(any(ReplicationTask.class));
        int totalProcessed = localOpsCounter.get() + incomingOpsCounter.get();
        assertEquals(totalOps, totalProcessed, "All operations must be accounted for");

        System.out.printf("✅ Mixed workload test:%n");
        System.out.printf("   Total operations: %,d%n", totalOps);
        System.out.printf("   Local operations: %,d%n", localOpsCounter.get());
        System.out.printf("   Incoming operations: %,d%n", incomingOpsCounter.get());
    }

    @Test
    @DisplayName("Should handle rapid sequential operations without degradation")
    void shouldHandleRapidSequentialOperations() {
        int operationCount = 10_000;
        long startTime = System.nanoTime();

        for (int i = 0; i < operationCount; i++) {
            String key = "seq-key-" + i;
            String value = "seq-value-" + i;

            if (i % 2 == 0) {
                manager.onLocalSet(key, value);
            } else {
                manager.onLocalDelete(key);
            }
        }
        long durationMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime);
        verify(sender, atLeastOnce()).replicate(eq(self), any(ReplicationTask.class));

        System.out.printf("✅ Rapid sequential test:%n");
        System.out.printf("   Operations: %,d%n", operationCount);
        System.out.printf("   Duration: %,d ms%n", durationMs);
        System.out.printf("   Throughput: %.2f ops/sec%n", operationCount / (durationMs / 1000.0));
    }

    @Test
    @DisplayName("Should handle graceful shutdown under load")
    void shouldHandleGracefulShutdownUnderLoad() throws InterruptedException {
        int threads = 8;
        int opsPerThread = 500;

        ExecutorService executor = Executors.newFixedThreadPool(threads);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threads);

        for (int i = 0; i < threads; i++) {
            executor.submit(() -> {
                try {
                    startLatch.await();
                    for (int j = 0; j < opsPerThread; j++) {
                        manager.onLocalSet("key-" + j, "value-" + j);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (Exception e) {
                    // May occur if manager is shutting down
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        Thread.sleep(100);
        assertDoesNotThrow(() -> manager.shutdown(), "Shutdown must complete cleanly even under load");
        doneLatch.await(5, TimeUnit.SECONDS);
        executor.shutdownNow();
        verify(sender, times(1)).shutdown();
        System.out.println("✅ Graceful shutdown under load completed");
    }

    @Test
    @DisplayName("Should skip replication when not master for key")
    void shouldSkipReplicationWhenNotMaster() {
        NodeInfo otherNode = new NodeInfo("node-B", "127.0.0.1", 9001);
        Function<String, NodeInfo> resolver = key -> otherNode;

        ReplicationManager nonMasterManager = new ReplicationManager(
                self,
                sender,
                receiver,
                resolver
        );

        for (int i = 0; i < 100; i++) {
            nonMasterManager.onLocalSet("key-" + i, "value-" + i);
            nonMasterManager.onLocalDelete("key-" + i);
        }

        verify(sender, never()).replicate(any(), any());
        System.out.println("✅ Non-master skip test completed");
    }

    @Test
    @DisplayName("Should handle exceptions in receiver gracefully")
    void shouldHandleReceiverExceptionsGracefully() {
        doThrow(new RuntimeException("Simulated receiver error")).when(receiver).applyTask(any());

        assertDoesNotThrow(() -> {
            for (int i = 0; i < 100; i++) {
                ReplicationTask task = ReplicationTask.ofSet(
                        "k-" + i,
                        "v-" + i,
                        "node-B"
                );
                manager.onReplicationReceived(task);
            }
        });

        verify(receiver, times(100)).applyTask(any());
        System.out.println("✅ Exception handling test completed");
    }
}