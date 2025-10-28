package com.iksanov.distributedcache.node.core.stress;

import com.iksanov.distributedcache.node.core.InMemoryCacheStore;
import org.junit.jupiter.api.Test;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Stress tests for {@link InMemoryCacheStore}.
 * <p>
 * Ensures:
 *  - Thread-safe concurrent put/get/delete operations
 *  - TTL expiration under load
 *  - Size limit enforcement
 */
public class InMemoryCacheStoreStressTest {

    @Test
    void shouldHandleHighConcurrencyAndTTLExpiration() throws Exception {
        int maxSize = 5000;
        long ttlMillis = 200;
        long cleanupInterval = 100;
        InMemoryCacheStore store = new InMemoryCacheStore(maxSize, ttlMillis, cleanupInterval);

        int threads = 8;
        int operationsPerThread = 20000;
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threads);
        AtomicInteger counter = new AtomicInteger(0);
        Random random = new Random();

        for (int t = 0; t < threads; t++) {
            executor.submit(() -> {
                try {
                    startLatch.await();
                    for (int i = 0; i < operationsPerThread; i++) {
                        int id = random.nextInt(10_000);
                        String key = "k-" + id;
                        String value = "v-" + id;

                        int op = random.nextInt(100);
                        if (op < 60) {
                            store.put(key, value);
                        } else if (op < 90) {
                            store.get(key);
                        } else {
                            store.delete(key);
                        }

                        counter.incrementAndGet();
                    }
                } catch (Exception e) {
                    fail("Unexpected exception in worker: " + e);
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        doneLatch.await(30, TimeUnit.SECONDS);
        executor.shutdownNow();

        System.out.printf("✅ Completed %d concurrent operations%n", counter.get());
        System.out.printf("Final cache size: %d%n", store.size());

        Thread.sleep(ttlMillis * 2);

        long beforeCleanup = store.size();
        Thread.sleep(cleanupInterval * 2);
        long afterCleanup = store.size();

        System.out.printf("Cache size before cleanup: %d, after cleanup: %d%n",
                beforeCleanup, afterCleanup);

        assertTrue(afterCleanup <= beforeCleanup,
                "Cleanup must remove expired entries");

        assertTrue(store.size() <= maxSize,
                "Cache size should never exceed maxSize");
    }

    @Test
    void shouldEnforceMaxSizeUnderLoad() {
        int maxSize = 1000;
        InMemoryCacheStore store = new InMemoryCacheStore(maxSize, 1000, 500);

        for (int i = 0; i < 5000; i++) {
            store.put("k-" + i, "v-" + i);
        }

        System.out.printf("Cache size after heavy put: %d%n", store.size());
        assertTrue(store.size() <= maxSize,
                "Cache size should be capped by maxSize");
    }

    @Test
    void shouldSupportConcurrentDeletes() throws InterruptedException {
        InMemoryCacheStore store = new InMemoryCacheStore(1000, 10000, 500);
        int threads = 6;
        int operations = 10000;

        for (int i = 0; i < 1000; i++) {
            store.put("k-" + i, "v-" + i);
        }

        ExecutorService executor = Executors.newFixedThreadPool(threads);
        CountDownLatch latch = new CountDownLatch(threads);

        for (int t = 0; t < threads; t++) {
            executor.submit(() -> {
                try {
                    for (int i = 0; i < operations; i++) {
                        String key = "k-" + (i % 1000);
                        if (i % 3 == 0) store.delete(key);
                        else store.get(key);
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await(10, TimeUnit.SECONDS);
        executor.shutdownNow();

        System.out.printf("Final cache size after concurrent deletes: %d%n", store.size());
        assertTrue(store.size() <= 1000, "Cache must remain within size limit");
    }
}
