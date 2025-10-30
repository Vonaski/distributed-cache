package com.iksanov.distributedcache.node.core.stress;

import com.iksanov.distributedcache.node.core.InMemoryCacheStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Stress tests for {@link InMemoryCacheStore}.
 * <p>
 * Validates:
 *  - Thread-safe concurrent operations at high load
 *  - TTL expiration under stress
 *  - Strict size limit enforcement
 *  - Performance characteristics
 *  - Memory efficiency
 */
public class InMemoryCacheStoreStressTest {

    @Test
    @DisplayName("Should handle high concurrency with TTL expiration")
    void shouldHandleHighConcurrencyAndTTLExpiration() throws Exception {
        int maxSize = 5000;
        long ttlMillis = 200;
        long cleanupInterval = 100;
        InMemoryCacheStore store = new InMemoryCacheStore(maxSize, ttlMillis, cleanupInterval);

        int threads = 16;
        int operationsPerThread = 20000;
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threads);
        AtomicLong totalOps = new AtomicLong(0);
        AtomicInteger maxSizeViolations = new AtomicInteger(0);
        Random random = new Random();

        long startTime = System.nanoTime();

        for (int t = 0; t < threads; t++) {
            executor.submit(() -> {
                try {
                    startLatch.await();
                    for (int i = 0; i < operationsPerThread; i++) {
                        int id = random.nextInt(10_000);
                        String key = "k-" + id;
                        String value = "v-" + id + "-" + System.nanoTime();

                        int op = random.nextInt(100);
                        if (op < 60) {
                            // 60% writes
                            store.put(key, value);

                            // Verify size constraint
                            int currentSize = store.size();
                            if (currentSize > maxSize) {
                                maxSizeViolations.incrementAndGet();
                                System.err.printf("Size violation: %d > %d%n", currentSize, maxSize);
                            }
                        } else if (op < 90) {
                            // 30% reads
                            store.get(key);
                        } else {
                            // 10% deletes
                            store.delete(key);
                        }

                        totalOps.incrementAndGet();
                    }
                } catch (Exception e) {
                    fail("Unexpected exception in worker: " + e);
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        boolean completed = doneLatch.await(30, TimeUnit.SECONDS);
        assertTrue(completed, "All threads should complete within timeout");

        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime);
        executor.shutdownNow();

        // Verify invariants
        assertEquals(0, maxSizeViolations.get(),
                "Size should NEVER exceed maxSize with synchronous eviction");

        int finalSize = store.size();
        assertTrue(finalSize <= maxSize,
                "Final size " + finalSize + " must be <= maxSize " + maxSize);

        // Performance metrics
        double throughput = totalOps.get() / (elapsedMs / 1000.0);
        System.out.printf("✅ Completed %,d operations in %d ms (%.0f ops/sec)%n",
                totalOps.get(), elapsedMs, throughput);
        System.out.printf("Final cache size: %d (max: %d)%n", finalSize, maxSize);

        // Wait for TTL cleanup
        Thread.sleep(ttlMillis * 2 + cleanupInterval * 2);

        int sizeAfterCleanup = store.size();
        System.out.printf("Size after TTL cleanup: %d%n", sizeAfterCleanup);

        // Most entries should be expired and cleaned up
        assertTrue(sizeAfterCleanup < finalSize,
                "Cleanup should remove expired entries");

        store.shutdown();
    }

    @Test
    @DisplayName("Should strictly enforce max size under extreme load")
    void shouldEnforceMaxSizeUnderExtremeLoad() throws InterruptedException {
        int maxSize = 1000;
        InMemoryCacheStore store = new InMemoryCacheStore(maxSize, 0, 10000); // No TTL

        int threads = 20;
        int operations = 50000;
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        CountDownLatch latch = new CountDownLatch(threads);
        AtomicInteger sizeViolations = new AtomicInteger(0);

        long startTime = System.nanoTime();

        for (int t = 0; t < threads; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    Random rand = new Random();
                    for (int i = 0; i < operations; i++) {
                        store.put("key-" + rand.nextInt(100000), "value-" + threadId + "-" + i);

                        // Check size after EVERY put
                        int size = store.size();
                        if (size > maxSize) {
                            sizeViolations.incrementAndGet();
                            System.err.printf("Thread %d: Size violation %d > %d at op %d%n",
                                    threadId, size, maxSize, i);
                        }
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        boolean completed = latch.await(60, TimeUnit.SECONDS);
        assertTrue(completed, "All threads must complete");

        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime);
        executor.shutdownNow();

        assertEquals(0, sizeViolations.get(),
                "No size violations allowed with synchronous eviction");

        int finalSize = store.size();
        assertEquals(maxSize, finalSize,
                "Cache should be exactly at maxSize after heavy load");

        long totalOps = (long) threads * operations;
        double throughput = totalOps / (elapsedMs / 1000.0);
        System.out.printf("✅ Extreme load test: %,d ops in %d ms (%.0f ops/sec)%n",
                totalOps, elapsedMs, throughput);
        System.out.printf("Final size: %d, Size violations: %d%n",
                finalSize, sizeViolations.get());

        store.shutdown();
    }

    @Test
    @DisplayName("Should handle mixed workload with deletions")
    void shouldHandleMixedWorkloadWithDeletions() throws InterruptedException {
        InMemoryCacheStore store = new InMemoryCacheStore(1000, 10000, 500);

        // Pre-populate cache
        for (int i = 0; i < 1000; i++) {
            store.put("initial-" + i, "value-" + i);
        }
        assertEquals(1000, store.size());

        int threads = 12;
        int operations = 10000;
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        CountDownLatch latch = new CountDownLatch(threads);
        AtomicLong getHits = new AtomicLong(0);
        AtomicLong getMisses = new AtomicLong(0);
        AtomicInteger maxObservedSize = new AtomicInteger(0);

        for (int t = 0; t < threads; t++) {
            executor.submit(() -> {
                try {
                    Random rand = new Random();
                    for (int i = 0; i < operations; i++) {
                        String key = "key-" + rand.nextInt(2000);

                        int op = rand.nextInt(100);
                        if (op < 40) {
                            // 40% puts
                            store.put(key, "v-" + i);
                        } else if (op < 70) {
                            // 30% gets
                            String value = store.get(key);
                            if (value != null) getHits.incrementAndGet();
                            else getMisses.incrementAndGet();
                        } else if (op < 90) {
                            // 20% deletes
                            store.delete(key);
                        } else {
                            // 10% get initial keys
                            String value = store.get("initial-" + rand.nextInt(1000));
                            if (value != null) getHits.incrementAndGet();
                            else getMisses.incrementAndGet();
                        }

                        maxObservedSize.updateAndGet(current -> Math.max(current, store.size()));
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        boolean completed = latch.await(30, TimeUnit.SECONDS);
        assertTrue(completed);
        executor.shutdownNow();

        int finalSize = store.size();
        assertTrue(finalSize <= 1000, "Final size must respect limit");
        assertTrue(maxObservedSize.get() <= 1000,
                "Size should never exceed limit: " + maxObservedSize.get());

        double hitRate = getHits.get() * 100.0 / (getHits.get() + getMisses.get());
        System.out.printf("✅ Mixed workload: Final size=%d, Max observed=%d%n",
                finalSize, maxObservedSize.get());
        System.out.printf("Cache hit rate: %.2f%% (hits=%d, misses=%d)%n",
                hitRate, getHits.get(), getMisses.get());

        store.shutdown();
    }

    @Test
    @DisplayName("Should demonstrate LRU effectiveness under memory pressure")
    void shouldDemonstrateLRUEffectiveness() {
        int maxSize = 100;
        InMemoryCacheStore store = new InMemoryCacheStore(maxSize, 0, 10000); // No TTL

        // Insert items with clear access pattern
        for (int i = 0; i < 200; i++) {
            store.put("key-" + i, "value-" + i);
        }

        // Only keys 100-199 should remain (most recent)
        for (int i = 0; i < 100; i++) {
            assertNull(store.get("key-" + i),
                    "Old key-" + i + " should be evicted");
        }
        for (int i = 100; i < 200; i++) {
            assertEquals("value-" + i, store.get("key-" + i),
                    "Recent key-" + i + " should be present");
        }

        // Access some middle keys to make them "hot"
        for (int i = 150; i < 160; i++) {
            store.get("key-" + i); // Touch these keys
        }

        // Add more keys - should evict untouched recent keys, not hot ones
        for (int i = 200; i < 250; i++) {
            store.put("key-" + i, "value-" + i);
        }

        // Hot keys 150-159 should still be present
        for (int i = 150; i < 160; i++) {
            assertNotNull(store.get("key-" + i),
                    "Hot key-" + i + " should not be evicted");
        }

        System.out.println("✅ LRU eviction working correctly - hot keys preserved");
        store.shutdown();
    }

    @Test
    @DisplayName("Performance benchmark - measure operation latencies")
    void performanceBenchmark() {
        int maxSize = 10000;
        InMemoryCacheStore store = new InMemoryCacheStore(maxSize, 0, 10000);

        // Warm up
        for (int i = 0; i < maxSize; i++) {
            store.put("warmup-" + i, "value-" + i);
        }

        int operations = 100000;
        Random random = new Random();

        // Measure PUT performance
        long putStart = System.nanoTime();
        for (int i = 0; i < operations; i++) {
            store.put("bench-" + random.nextInt(maxSize * 2), "value-" + i);
        }
        long putTime = System.nanoTime() - putStart;

        // Measure GET performance (mostly hits)
        long getHitStart = System.nanoTime();
        for (int i = 0; i < operations; i++) {
            store.get("warmup-" + random.nextInt(maxSize));
        }
        long getHitTime = System.nanoTime() - getHitStart;

        // Measure GET performance (mostly misses)
        long getMissStart = System.nanoTime();
        for (int i = 0; i < operations; i++) {
            store.get("missing-" + random.nextInt(maxSize * 2));
        }
        long getMissTime = System.nanoTime() - getMissStart;

        // Measure DELETE performance
        long deleteStart = System.nanoTime();
        for (int i = 0; i < operations; i++) {
            store.delete("bench-" + random.nextInt(maxSize * 2));
        }
        long deleteTime = System.nanoTime() - deleteStart;

        // Calculate throughput
        double putOpsPerSec = operations / (putTime / 1_000_000_000.0);
        double getHitOpsPerSec = operations / (getHitTime / 1_000_000_000.0);
        double getMissOpsPerSec = operations / (getMissTime / 1_000_000_000.0);
        double deleteOpsPerSec = operations / (deleteTime / 1_000_000_000.0);

        System.out.println("=== Performance Benchmark Results ===");
        System.out.printf("PUT:        %,d ops in %d ms (%.0f ops/sec, %.0f ns/op)%n",
                operations, putTime / 1_000_000, putOpsPerSec, putTime / (double) operations);
        System.out.printf("GET (hit):  %,d ops in %d ms (%.0f ops/sec, %.0f ns/op)%n",
                operations, getHitTime / 1_000_000, getHitOpsPerSec, getHitTime / (double) operations);
        System.out.printf("GET (miss): %,d ops in %d ms (%.0f ops/sec, %.0f ns/op)%n",
                operations, getMissTime / 1_000_000, getMissOpsPerSec, getMissTime / (double) operations);
        System.out.printf("DELETE:     %,d ops in %d ms (%.0f ops/sec, %.0f ns/op)%n",
                operations, deleteTime / 1_000_000, deleteOpsPerSec, deleteTime / (double) operations);

        store.shutdown();
    }
}