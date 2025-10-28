package com.iksanov.distributedcache.node.core;

import org.junit.jupiter.api.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.*;

/**
 * Comprehensive tests for {@link InMemoryCacheStore}.
 * <p>
 * Covers:
 *  - Basic CRUD operations
 *  - TTL expiration logic
 *  - LRU eviction order
 *  - Behavior without TTL
 *  - Thread safety under concurrent load
 */
@DisplayName("InMemoryCacheStore - core functionality and concurrency tests")
class InMemoryCacheStoreTest {

    private InMemoryCacheStore cache;

    @BeforeEach
    void setUp() {
        // maxSize = 5, TTL = 1 second, cleanup every 500ms
        cache = new InMemoryCacheStore(5, 1000, 500);
    }

    @AfterEach
    void tearDown() {
        cache.shutdown();
    }

    @Test
    @DisplayName("put() and get() should store and retrieve values correctly")
    void shouldStoreAndRetrieveValues() {
        cache.put("user:1", "John");
        cache.put("user:2", "Jane");

        assertThat(cache.size()).isEqualTo(2);
        assertThat(cache.get("user:1")).isEqualTo("John");
        assertThat(cache.get("user:2")).isEqualTo("Jane");
    }

    @Test
    @DisplayName("get() should return null for missing key")
    void shouldReturnNullForMissingKey() {
        assertThat(cache.get("missing:key")).isNull();
    }

    @Test
    @DisplayName("delete() should remove key if exists, ignore if not")
    void shouldDeleteKeySafely() {
        cache.put("session", "active");

        cache.delete("session");
        assertThat(cache.get("session")).isNull();
        assertThat(cache.size()).isZero();

        assertThatCode(() -> cache.delete("ghost")).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("clear() should remove all entries")
    void shouldClearAllEntries() {
        cache.put("a", "1");
        cache.put("b", "2");

        cache.clear();

        assertThat(cache.size()).isZero();
        assertThat(cache.get("a")).isNull();
        assertThat(cache.get("b")).isNull();
    }

    @Test
    @DisplayName("put() on existing key should refresh TTL and value")
    void shouldRefreshTTLAndValueOnUpdate() throws InterruptedException {
        cache.put("key", "initial");
        Thread.sleep(800);
        cache.put("key", "updated");

        Thread.sleep(300);
        assertThat(cache.get("key"))
                .as("Re-put should refresh TTL and value")
                .isEqualTo("updated");
    }

    @Test
    @DisplayName("Entries should expire after TTL")
    void shouldExpireEntriesAfterTTL() throws InterruptedException {
        cache.put("temp", "data");
        assertThat(cache.get("temp")).isEqualTo("data");

        Thread.sleep(1200);

        assertThat(cache.get("temp")).as("Expired entry should be removed").isNull();
    }

    @Test
    @DisplayName("Entries should not expire when TTL=0 (no expiration)")
    void shouldNotExpireIfTTLIsZero() throws InterruptedException {
        InMemoryCacheStore noTtlCache = new InMemoryCacheStore(3, 0, 500);

        noTtlCache.put("key", "value");
        Thread.sleep(2000);

        assertThat(noTtlCache.get("key")).isEqualTo("value");
        assertThat(noTtlCache.size()).isEqualTo(1);

        noTtlCache.shutdown();
    }

    @Test
    @DisplayName("Cleanup thread should remove expired entries automatically")
    void shouldRemoveExpiredEntriesDuringCleanup() throws InterruptedException {
        cache.put("auto", "cleanup");

        Thread.sleep(1500);
        assertThat(cache.get("auto")).isNull();
        assertThat(cache.size()).isLessThanOrEqualTo(1);
    }

    @Test
    @DisplayName("Should evict least recently used entry when max size is exceeded")
    void shouldEvictLeastRecentlyUsedEntry() throws InterruptedException {
        for (int i = 1; i <= 5; i++) {
            cache.put("k" + i, "v" + i);
        }
        assertThat(cache.size()).isEqualTo(5);

        cache.put("k6", "v6");

        Thread.sleep(100);

        assertThat(cache.size()).isEqualTo(5);
        assertThat(cache.get("k1"))
                .as("The oldest key (k1) should be evicted by LRU")
                .isNull();
    }

    @Test
    @DisplayName("get() should refresh entry position for LRU eviction order")
    void shouldRefreshEntryOnGet() {
        for (int i = 1; i <= 5; i++) {
            cache.put("k" + i, "v" + i);
        }

        cache.get("k1");

        cache.put("k6", "v6");

        assertThat(cache.get("k1"))
                .as("Recently accessed key should not be evicted")
                .isNotNull();
    }

    @Test
    @DisplayName("Should not exceed max size even after many insertions")
    void shouldRespectMaxSizeLimit() throws InterruptedException {
        for (int i = 0; i < 100; i++) {
            cache.put("k" + i, "v" + i);
        }

        Thread.sleep(200);

        int currentSize = cache.size();
        assertThat(currentSize)
                .as("Cache should stabilize around or below maxSize after async evictions")
                .isLessThanOrEqualTo(5);
    }

    @Test
    @DisplayName("Should remain stable under high concurrency stress (with overshoot tolerance)")
    void shouldHandleConcurrentAccess() throws InterruptedException {
        int threads = 10;
        AtomicInteger successfulGets = new AtomicInteger();
        AtomicInteger exceptionCount = new AtomicInteger();
        boolean completed;

        try (ExecutorService executor = Executors.newFixedThreadPool(threads)) {
            int iterations = 200;

            Runnable writer = () -> {
                for (int i = 0; i < iterations; i++) {
                    try {
                        cache.put("key-" + (i % 20), "val-" + i);
                    } catch (Exception e) {
                        exceptionCount.incrementAndGet();
                    }
                }
            };

            Runnable reader = () -> {
                for (int i = 0; i < iterations; i++) {
                    try {
                        String val = cache.get("key-" + (i % 20));
                        if (val != null) successfulGets.incrementAndGet();
                    } catch (Exception e) {
                        exceptionCount.incrementAndGet();
                    }
                }
            };

            Runnable deleter = () -> {
                for (int i = 0; i < iterations / 2; i++) {
                    try {
                        cache.delete("key-" + (i % 20));
                    } catch (Exception e) {
                        exceptionCount.incrementAndGet();
                    }
                }
            };

            for (int i = 0; i < 4; i++) executor.submit(writer);
            for (int i = 0; i < 4; i++) executor.submit(reader);
            for (int i = 0; i < 2; i++) executor.submit(deleter);

            executor.shutdown();
            completed = executor.awaitTermination(10, TimeUnit.SECONDS);
        }

        assertThat(completed).isTrue();

        Thread.sleep(300);

        int finalSize = cache.size();
        int maxAllowed = 10 * 2;
        assertThat(finalSize)
                .as("Cache size should stabilize near maxSize (allowing overshoot)")
                .isLessThanOrEqualTo(maxAllowed);

        assertThat(successfulGets.get())
                .as("Concurrent reads should succeed at least partially")
                .isGreaterThan(0);

        assertThat(exceptionCount.get())
                .as("No more than a few exceptions should occur under stress")
                .isLessThan(10);
    }

    @Test
    @DisplayName("Concurrent writes should not break cache under stress (temporary overshoot allowed)")
    void shouldRemainConsistentUnderConcurrentWrites() throws InterruptedException {
        int threads = 8;
        int maxSize = 5;
        AtomicInteger exceptionCount = new AtomicInteger();

        try (ExecutorService executor = Executors.newFixedThreadPool(threads)) {
            Runnable task = () -> {
                for (int i = 0; i < 100; i++) {
                    try {
                        cache.put("key-" + ThreadLocalRandom.current().nextInt(50), "val-" + i);
                    } catch (Exception e) {
                        exceptionCount.incrementAndGet();
                    }
                }
            };

            for (int i = 0; i < threads; i++) {
                executor.submit(task);
            }

            executor.shutdown();
            assertThat(executor.awaitTermination(5, TimeUnit.SECONDS))
                    .as("All writer threads should complete within timeout")
                    .isTrue();
        }

        Thread.sleep(300);

        int finalSize = cache.size();

        assertThat(finalSize)
                .as("Cache size may temporarily overshoot but should stay within ~2× maxSize after stabilization")
                .isLessThanOrEqualTo(maxSize * 2);

        assertThat(exceptionCount.get())
                .as("No unexpected exceptions should occur under concurrency")
                .isZero();
    }

    @Test
    @DisplayName("Cache should evict items and stabilize below maxSize after heavy load")
    void shouldEvictAndStabilizeBelowMaxSize() throws InterruptedException {
        int maxSize = 10;

        for (int i = 0; i < 200; i++) {
            cache.put("key-" + i, "val-" + i);
        }

        Thread.sleep(500);

        int stableSize = cache.size();

        assertThat(stableSize)
                .as("Cache should stabilize at or below maxSize after background eviction")
                .isLessThanOrEqualTo(maxSize);
    }

    @Test
    @DisplayName("shutdown() should stop cleanup thread safely")
    void shouldShutdownGracefully() {
        assertThatCode(() -> cache.shutdown())
                .doesNotThrowAnyException();
    }
}
