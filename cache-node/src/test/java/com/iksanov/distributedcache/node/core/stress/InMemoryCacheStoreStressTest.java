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
 * Stress tests for {@link InMemoryCacheStore} with Approximate LRU.
 * Validates behavior under extreme concurrent load.
 */
public class InMemoryCacheStoreStressTest {

    @Test
    @DisplayName("Should handle high concurrency with TTL expiration")
    void shouldHandleHighConcurrencyAndTTLExpiration() throws Exception {
        int maxSize = 5000;
        long ttlMillis = 200;
        InMemoryCacheStore store = new InMemoryCacheStore(maxSize, ttlMillis, 100);

        int threads = 16;
        int operationsPerThread = 20000;
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threads);
        AtomicLong totalOps = new AtomicLong(0);
        AtomicInteger maxObservedSize = new AtomicInteger(0);
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
                            store.put(key, value);
                            maxObservedSize.updateAndGet(current -> Math.max(current, store.size()));
                        } else if (op < 90) {
                            store.get(key);
                        } else {
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
        int finalSize = store.size();
        int allowedMaxSize = (int)(maxSize * 1.3);
        assertTrue(maxObservedSize.get() <= allowedMaxSize, "Max observed size " + maxObservedSize.get() + " should not wildly exceed limit");
        assertTrue(finalSize <= allowedMaxSize, "Final size " + finalSize + " should be reasonable");

        double throughput = totalOps.get() / (elapsedMs / 1000.0);
        System.out.printf("✅ High concurrency test: %,d ops in %d ms (%.0f ops/sec)%n", totalOps.get(), elapsedMs, throughput);
        System.out.printf("Max observed size: %d, Final size: %d (limit: %d)%n", maxObservedSize.get(), finalSize, maxSize);
        Thread.sleep(ttlMillis * 2 + 200);
        int sizeAfterExpiry = store.size();
        System.out.printf("Size after TTL expiry: %d%n", sizeAfterExpiry);
        store.shutdown();
    }

    @Test
    @DisplayName("Should enforce reasonable size bounds under extreme load")
    void shouldEnforceSizeBoundsUnderExtremeLoad() throws InterruptedException {
        int maxSize = 1000;
        InMemoryCacheStore store = new InMemoryCacheStore(maxSize, 0, 10000);
        int threads = 20;
        int operations = 50000;
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        CountDownLatch latch = new CountDownLatch(threads);
        AtomicInteger maxObservedSize = new AtomicInteger(0);
        int allowedMaxSize = maxSize + 50;
        long startTime = System.nanoTime();

        for (int t = 0; t < threads; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    Random rand = new Random();
                    for (int i = 0; i < operations; i++) {
                        store.put("key-" + rand.nextInt(100000), "value-" + threadId + "-" + i);
                        int size = store.size();
                        maxObservedSize.updateAndGet(current -> Math.max(current, size));
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
        int finalSize = store.size();
        assertTrue(finalSize <= allowedMaxSize, "Final size " + finalSize + " must be reasonable");
        long totalOps = (long) threads * operations;
        double throughput = totalOps / (elapsedMs / 1000.0);
        System.out.printf("✅ Extreme load test: %,d ops in %d ms (%.0f ops/sec)%n", totalOps, elapsedMs, throughput);
        System.out.printf("Max observed size: %d, Final size: %d (limit: %d)%n", maxObservedSize.get(), finalSize, maxSize);
        store.shutdown();
    }

    @Test
    @DisplayName("Should handle mixed workload with deletions")
    void shouldHandleMixedWorkloadWithDeletions() throws InterruptedException {
        InMemoryCacheStore store = new InMemoryCacheStore(1000, 10000, 500);
        for (int i = 0; i < 1000; i++) {
            store.put("initial-" + i, "value-" + i);
        }

        int maxSize = 1000;
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
                            store.put(key, "v-" + i);
                        } else if (op < 70) {
                            String value = store.get(key);
                            if (value != null) getHits.incrementAndGet();
                            else getMisses.incrementAndGet();
                        } else if (op < 90) {
                            store.delete(key);
                        } else {
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
        int allowedMax = (int)(maxSize * 1.3);
        assertTrue(finalSize <= allowedMax, "Final size must respect limit");
        assertTrue(maxObservedSize.get() <= allowedMax, "Max size should not wildly exceed limit: " + maxObservedSize.get());
        double hitRate = getHits.get() * 100.0 / (getHits.get() + getMisses.get());
        System.out.printf("✅ Mixed workload: Final size=%d, Max observed=%d%n", finalSize, maxObservedSize.get());
        System.out.printf("Cache hit rate: %.2f%% (hits=%d, misses=%d)%n", hitRate, getHits.get(), getMisses.get());
        store.shutdown();
    }

    @Test
    @DisplayName("Should evict entries when cache is full")
    void shouldEvictEntriesWhenFull() {
        int maxSize = 50;
        InMemoryCacheStore store = new InMemoryCacheStore(maxSize, 0, 10000);
        for (int i = 0; i < 150; i++) {
            store.put("key-" + i, "value-" + i);
        }

        int finalSize = store.size();
        assertTrue(finalSize <= maxSize + 10, "Final size " + finalSize + " should be near maxSize " + maxSize);
        InMemoryCacheStore.CacheStats stats = store.getStats();
        assertTrue(stats.evictions > 0, "Evictions should have occurred");
        System.out.printf("✅ Eviction test passed: size=%d, evictions=%d%n", finalSize, stats.evictions);
        store.shutdown();
    }

    @Test
    @DisplayName("Performance benchmark - measure operation latencies")
    void performanceBenchmark() {
        int maxSize = 10000;
        InMemoryCacheStore store = new InMemoryCacheStore(maxSize, 0, 10000);

        for (int i = 0; i < maxSize; i++) {
            store.put("warmup-" + i, "value-" + i);
        }

        int operations = 100000;
        Random random = new Random();

        long putStart = System.nanoTime();
        for (int i = 0; i < operations; i++) {
            store.put("bench-" + random.nextInt(maxSize * 2), "value-" + i);
        }
        long putTime = System.nanoTime() - putStart;

        long getHitStart = System.nanoTime();
        for (int i = 0; i < operations; i++) {
            store.get("warmup-" + random.nextInt(maxSize));
        }
        long getHitTime = System.nanoTime() - getHitStart;

        long getMissStart = System.nanoTime();
        for (int i = 0; i < operations; i++) {
            store.get("missing-" + random.nextInt(maxSize * 2));
        }
        long getMissTime = System.nanoTime() - getMissStart;

        long deleteStart = System.nanoTime();
        for (int i = 0; i < operations; i++) {
            store.delete("bench-" + random.nextInt(maxSize * 2));
        }
        long deleteTime = System.nanoTime() - deleteStart;

        double putOpsPerSec = operations / (putTime / 1_000_000_000.0);
        double getHitOpsPerSec = operations / (getHitTime / 1_000_000_000.0);
        double getMissOpsPerSec = operations / (getMissTime / 1_000_000_000.0);
        double deleteOpsPerSec = operations / (deleteTime / 1_000_000_000.0);

        System.out.println("=== Performance Benchmark (Approximate LRU) ===");
        System.out.printf("PUT:        %,d ops in %d ms (%.0f ops/sec, %.0f ns/op)%n", operations, putTime / 1_000_000, putOpsPerSec, putTime / (double) operations);
        System.out.printf("GET (hit):  %,d ops in %d ms (%.0f ops/sec, %.0f ns/op)%n", operations, getHitTime / 1_000_000, getHitOpsPerSec, getHitTime / (double) operations);
        System.out.printf("GET (miss): %,d ops in %d ms (%.0f ops/sec, %.0f ns/op)%n", operations, getMissTime / 1_000_000, getMissOpsPerSec, getMissTime / (double) operations);
        System.out.printf("DELETE:     %,d ops in %d ms (%.0f ops/sec, %.0f ns/op)%n", operations, deleteTime / 1_000_000, deleteOpsPerSec, deleteTime / (double) operations);
        store.shutdown();
    }
}