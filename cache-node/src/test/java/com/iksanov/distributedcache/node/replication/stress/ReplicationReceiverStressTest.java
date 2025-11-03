package com.iksanov.distributedcache.node.replication.stress;

import com.iksanov.distributedcache.node.core.InMemoryCacheStore;
import com.iksanov.distributedcache.node.metrics.CacheMetrics;
import com.iksanov.distributedcache.node.replication.ReplicationReceiver;
import com.iksanov.distributedcache.node.replication.ReplicationTask;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Stress test for ReplicationReceiver under heavy concurrent load.
 * <p>
 * Tests receiver's ability to:
 * - Process many concurrent replication tasks
 * - Maintain consistency under high load
 * - Handle both SET and DELETE operations
 */
public class ReplicationReceiverStressTest {

    private final CacheMetrics metrics = new CacheMetrics();

    @Test
    @DisplayName("Should handle high concurrent load with consistency")
    void shouldHandleHighConcurrentLoadWithConsistency() throws Exception {
        InMemoryCacheStore store = new InMemoryCacheStore(10_000, 60_000, 10_000,  metrics);
        ReplicationReceiver receiver = new ReplicationReceiver("127.0.0.1", 0, store);

        final int threads = 8;
        final int operationsPerThread = 12_500;
        final int totalOperations = threads * operationsPerThread;

        ExecutorService executor = Executors.newFixedThreadPool(threads);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threads);
        AtomicLong successCounter = new AtomicLong(0);
        AtomicLong errorCounter = new AtomicLong(0);

        List<Future<?>> futures = new ArrayList<>();

        for (int t = 0; t < threads; t++) {
            final int threadId = t;
            futures.add(executor.submit(() -> {
                try {
                    startLatch.await();
                    for (int i = 0; i < operationsPerThread; i++) {
                        String key = "key-" + (i % 100);
                        String value = "v-" + threadId + "-" + i;
                        try {
                            ReplicationTask task = ReplicationTask.ofSet(
                                    key,
                                    value,
                                    "node-B"
                            );
                            receiver.applyTask(task);
                            successCounter.incrementAndGet();
                        } catch (Exception e) {
                            errorCounter.incrementAndGet();
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    fail("Thread interrupted: " + e.getMessage());
                } finally {
                    doneLatch.countDown();
                }
            }));
        }

        long startTime = System.nanoTime();
        startLatch.countDown();

        boolean finished = doneLatch.await(60, TimeUnit.SECONDS);
        assertTrue(finished, "All worker threads must finish within timeout");

        for (Future<?> f : futures) {
            f.get(10, TimeUnit.SECONDS);
        }

        long durationMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime);
        executor.shutdown();
        assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS), "Executor must shut down cleanly");

        assertTrue(store.size() > 0, "Cache should contain entries after replication tasks");

        for (int i = 0; i < 100; i++) {
            String key = "key-" + i;
            String val = store.get(key);
            assertNotNull(val, "Cache value for " + key + " should not be null");
        }

        long totalProcessed = successCounter.get() + errorCounter.get();
        assertEquals(totalOperations, totalProcessed, "All operations must be accounted for");

        System.out.printf("✅ Receiver stress test completed:%n");
        System.out.printf("   Total operations: %,d%n", totalOperations);
        System.out.printf("   Duration: %,d ms%n", durationMs);
        System.out.printf("   Success: %,d (%.2f%%)%n", successCounter.get(), 100.0 * successCounter.get() / totalOperations);
        System.out.printf("   Errors: %,d (%.2f%%)%n", errorCounter.get(), 100.0 * errorCounter.get() / totalOperations);
        System.out.printf("   Final cache size: %d%n", store.size());
        System.out.printf("   Throughput: %.2f ops/sec%n", totalOperations / (durationMs / 1000.0));
    }

    @Test
    @DisplayName("Should handle mixed SET and DELETE operations under load")
    void shouldHandleMixedOperationsUnderLoad() throws Exception {
        InMemoryCacheStore store = new InMemoryCacheStore(5_000, 60_000, 10_000,  metrics);
        ReplicationReceiver receiver = new ReplicationReceiver("127.0.0.1", 0, store);

        final int threads = 6;
        final int operationsPerThread = 5_000;

        ExecutorService executor = Executors.newFixedThreadPool(threads);
        CountDownLatch latch = new CountDownLatch(threads);
        AtomicLong setOperations = new AtomicLong(0);
        AtomicLong deleteOperations = new AtomicLong(0);

        for (int t = 0; t < threads; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    for (int i = 0; i < operationsPerThread; i++) {
                        String key = "key-" + (i % 500);
                        ReplicationTask task;
                        if (i % 4 == 0) {
                            task = ReplicationTask.ofDelete(key, "node-" + threadId);
                            deleteOperations.incrementAndGet();
                        } else {
                            task = ReplicationTask.ofSet(
                                    key,
                                    "value-" + threadId + "-" + i,
                                    "node-" + threadId
                            );
                            setOperations.incrementAndGet();
                        }
                        receiver.applyTask(task);
                    }
                } finally {
                    latch.countDown();
                }
            });
        }
        boolean finished = latch.await(60, TimeUnit.SECONDS);
        executor.shutdownNow();
        assertTrue(finished, "All operations must complete");

        System.out.printf("✅ Mixed operations test:%n");
        System.out.printf("   SET operations: %,d%n", setOperations.get());
        System.out.printf("   DELETE operations: %,d%n", deleteOperations.get());
        System.out.printf("   Final cache size: %d%n", store.size());
    }

    @Test
    @DisplayName("Should handle rapid sequential operations")
    void shouldHandleRapidSequentialOperations() {
        InMemoryCacheStore store = new InMemoryCacheStore(10_000, 60_000, 10_000,  metrics);
        ReplicationReceiver receiver = new ReplicationReceiver("127.0.0.1", 0, store);

        int operationCount = 50_000;
        long startTime = System.nanoTime();

        for (int i = 0; i < operationCount; i++) {
            String key = "seq-key-" + (i % 1000);
            String value = "value-" + i;
            ReplicationTask task = ReplicationTask.ofSet(key, value, "origin");
            receiver.applyTask(task);
        }

        long durationMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime);

        assertTrue(store.size() > 0, "Store must contain data");
        assertTrue(store.size() <= 1000, "Store should not exceed key range");

        System.out.printf("✅ Sequential operations test:%n");
        System.out.printf("   Operations: %,d%n", operationCount);
        System.out.printf("   Duration: %,d ms%n", durationMs);
        System.out.printf("   Cache size: %d%n", store.size());
        System.out.printf("   Throughput: %.2f ops/sec%n", operationCount / (durationMs / 1000.0));
    }

    @Test
    @DisplayName("Should handle null task gracefully")
    void shouldHandleNullTaskGracefully() {
        InMemoryCacheStore store = new InMemoryCacheStore(100, 0, 500,  metrics);
        ReplicationReceiver receiver = new ReplicationReceiver("127.0.0.1", 0, store);
        assertDoesNotThrow(() -> receiver.applyTask(null));
    }

    @Test
    @DisplayName("Should maintain consistency with concurrent updates to same key")
    void shouldMaintainConsistencyWithConcurrentUpdates() throws Exception {
        InMemoryCacheStore store = new InMemoryCacheStore(100, 0, 500,  metrics);
        ReplicationReceiver receiver = new ReplicationReceiver("127.0.0.1", 0, store);

        String key = "concurrent-key";
        int threads = 10;
        int operationsPerThread = 100;

        ExecutorService executor = Executors.newFixedThreadPool(threads);
        CountDownLatch latch = new CountDownLatch(threads);

        for (int t = 0; t < threads; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    for (int i = 0; i < operationsPerThread; i++) {
                        String value = "thread-" + threadId + "-op-" + i;
                        ReplicationTask task = ReplicationTask.ofSet(
                                key,
                                value,
                                "node-" + threadId
                        );
                        receiver.applyTask(task);
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        assertTrue(latch.await(30, TimeUnit.SECONDS), "All threads must complete");
        executor.shutdownNow();

        String finalValue = store.get(key);
        assertNotNull(finalValue, "Key must have a value after all operations");
        assertTrue(finalValue.startsWith("thread-"), "Final value must be from one of the threads");
        System.out.printf("✅ Concurrent same-key test: final value = %s%n", finalValue);
    }
}