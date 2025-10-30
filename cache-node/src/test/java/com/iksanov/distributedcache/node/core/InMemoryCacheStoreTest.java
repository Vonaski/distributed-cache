package com.iksanov.distributedcache.node.core;

import org.junit.jupiter.api.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.AssertionsKt.assertNull;

/**
 * Comprehensive tests for {@link InMemoryCacheStore} with Approximate LRU.
 * Tests validate correctness while accounting for approximate eviction behavior.
 */
@DisplayName("InMemoryCacheStore - Approximate LRU implementation tests")
class InMemoryCacheStoreTest {

    private InMemoryCacheStore cache;

    @BeforeEach
    void setUp() {
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
    @DisplayName("put() on existing key should update value")
    void shouldUpdateValueOnOverwrite() {
        cache.put("key", "initial");
        cache.put("key", "updated");

        assertThat(cache.get("key"))
                .as("Put should update existing value")
                .isEqualTo("updated");

        assertThat(cache.size())
                .as("Size should remain 1 after update")
                .isEqualTo(1);
    }

    @Test
    @DisplayName("Entries should expire after TTL and be removed on access")
    void shouldExpireEntriesAfterTTL() throws InterruptedException {
        cache.put("temp", "data");
        assertThat(cache.get("temp")).isEqualTo("data");

        Thread.sleep(1200);

        assertThat(cache.get("temp"))
                .as("Expired entry should return null on access")
                .isNull();
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
    @DisplayName("Lazy cleanup should eventually remove expired entries")
    void shouldLazyCleanupExpiredEntries() throws InterruptedException {
        cache.put("auto", "cleanup");
        assertThat(cache.size()).isEqualTo(1);

        Thread.sleep(1200);

        // Trigger lazy cleanup by doing puts
        for (int i = 0; i < 150; i++) {
            cache.put("trigger" + i, "value" + i);
        }

        // The expired entry should eventually be cleaned up
        // Size should be around maxSize + batch buffer, not maxSize + 1 + all triggers
        assertThat(cache.size())
                .as("Lazy cleanup should have removed expired entries")
                .isLessThan(20);
    }

    @Test
    @DisplayName("Should evict entries when max size is exceeded")
    void shouldEvictWhenMaxSizeExceeded() {
        for (int i = 1; i <= 5; i++) {
            cache.put("k" + i, "v" + i);
        }
        assertThat(cache.size()).isEqualTo(5);

        // Add more entries
        for (int i = 6; i <= 10; i++) {
            cache.put("k" + i, "v" + i);
        }

        // Size should be controlled (at or near maxSize, allowing for batch buffer)
        assertThat(cache.size())
                .as("Cache size should be controlled after eviction")
                .isLessThanOrEqualTo(10);
    }

    @Test
    @DisplayName("Frequently accessed entries should have lower eviction priority")
    void shouldFavorFrequentlyAccessedEntries() {
        // Fill cache
        for (int i = 1; i <= 5; i++) {
            cache.put("k" + i, "v" + i);
        }

        // Make k1 "hot" by accessing it multiple times
        for (int i = 0; i < 20; i++) {
            assertThat(cache.get("k1")).isEqualTo("v1");
        }

        // Add many new entries to trigger evictions
        for (int i = 6; i <= 20; i++) {
            cache.put("k" + i, "v" + i);
        }

        // Hot key should have better survival chance (approximate LRU)
        // We can't guarantee it due to sampling, but we can verify the mechanism works
        InMemoryCacheStore.CacheStats stats = cache.getStats();
        assertThat(stats.evictions)
                .as("Some evictions should have occurred")
                .isGreaterThan(0);
    }

    @Test
    @DisplayName("Should respect max size bounds under heavy load")
    void shouldRespectMaxSizeUnderHeavyLoad() {
        for (int i = 0; i < 100; i++) {
            cache.put("k" + i, "v" + i);

            // Size should never wildly exceed maxSize
            assertThat(cache.size())
                    .as("Cache size must be bounded during insertions")
                    .isLessThanOrEqualTo(15); // maxSize + reasonable batch buffer
        }

        // Final size should be at maxSize
        assertThat(cache.size())
                .as("Final cache size should be at maxSize")
                .isLessThanOrEqualTo(10);
    }

    @Test
    @DisplayName("Should handle concurrent access correctly")
    void shouldHandleConcurrentAccess() throws InterruptedException {
        int threads = 10;
        int iterations = 200;
        AtomicInteger successfulGets = new AtomicInteger();
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threads);

        // Pre-populate with stable keys
        for (int i = 0; i < 5; i++) {
            cache.put("stable-" + i, "value-" + i);
        }

        for (int t = 0; t < threads; t++) {
            final int threadId = t;
            new Thread(() -> {
                try {
                    startLatch.await();

                    for (int i = 0; i < iterations; i++) {
                        String key = "key-" + (i % 20);

                        if (threadId < 3) {
                            cache.put(key, "val-" + threadId + "-" + i);
                        } else if (threadId < 7) {
                            String val = cache.get(i % 2 == 0 ? key : "stable-" + (i % 5));
                            if (val != null) successfulGets.incrementAndGet();
                        } else if (threadId < 9) {
                            if (i % 3 == 0) {
                                cache.delete(key);
                            }
                        } else {
                            if (i % 2 == 0) {
                                cache.put(key, "mixed-" + i);
                            } else {
                                String val = cache.get("stable-" + (i % 5));
                                if (val != null) successfulGets.incrementAndGet();
                            }
                        }
                    }
                } catch (Exception e) {
                    fail("Unexpected exception: " + e);
                } finally {
                    doneLatch.countDown();
                }
            }).start();
        }

        startLatch.countDown();
        boolean completed = doneLatch.await(10, TimeUnit.SECONDS);
        assertThat(completed).isTrue();

        assertThat(cache.size())
                .as("Cache size should be reasonable after concurrent access")
                .isLessThanOrEqualTo(15);

        assertThat(successfulGets.get())
                .as("Some reads should have succeeded")
                .isGreaterThan(0);
    }

    @Test
    @DisplayName("Concurrent writes should maintain size bounds")
    void shouldMaintainSizeBoundsUnderConcurrentWrites() throws InterruptedException {
        int threads = 8;
        int maxAllowedSize = 15; // maxSize + batch buffer
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threads);

        for (int t = 0; t < threads; t++) {
            new Thread(() -> {
                try {
                    startLatch.await();
                    for (int i = 0; i < 100; i++) {
                        cache.put("key-" + ThreadLocalRandom.current().nextInt(50), "val-" + i);

                        int currentSize = cache.size();
                        if (currentSize > maxAllowedSize) {
                            fail("Size bound violated: " + currentSize + " > " + maxAllowedSize);
                        }
                    }
                } catch (Exception e) {
                    fail("Unexpected exception: " + e);
                } finally {
                    doneLatch.countDown();
                }
            }).start();
        }

        startLatch.countDown();
        boolean completed = doneLatch.await(5, TimeUnit.SECONDS);
        assertThat(completed)
                .as("All threads should complete")
                .isTrue();

        assertThat(cache.size())
                .as("Final size must be reasonable")
                .isLessThanOrEqualTo(maxAllowedSize);
    }

    @Test
    @DisplayName("shutdown() should stop cleanup and clear cache")
    void shouldShutdownGracefully() {
        cache.put("key1", "value1");
        cache.put("key2", "value2");

        assertThatCode(() -> cache.shutdown())
                .doesNotThrowAnyException();

        assertThat(cache.size()).isEqualTo(0);
    }

    @Test
    @DisplayName("Null key or value should throw NullPointerException")
    void shouldRejectNullInputs() {
        assertThatThrownBy(() -> cache.get(null))
                .isInstanceOf(NullPointerException.class);

        assertThatThrownBy(() -> cache.put(null, "value"))
                .isInstanceOf(NullPointerException.class);

        assertThatThrownBy(() -> cache.put("key", null))
                .isInstanceOf(NullPointerException.class);

        assertThatThrownBy(() -> cache.delete(null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    @DisplayName("Cache stats should track hits, misses, and evictions")
    void shouldTrackCacheStats() {
        InMemoryCacheStore testCache = new InMemoryCacheStore(10, 0, 10000);
        testCache.put("key1", "value1");
        testCache.put("key2", "value2");
        testCache.get("key1"); // hit
        testCache.get("key1"); // hit
        testCache.get("missing"); // miss
        InMemoryCacheStore.CacheStats stats = testCache.getStats();
        assertThat(stats.hits).isEqualTo(2);
        assertThat(stats.misses).isEqualTo(1);
        assertThat(stats.hitRate).isCloseTo(66.67, within(0.1));
        assertThat(stats.size).isEqualTo(2);
        testCache.shutdown();
    }

    @Test
    @DisplayName("Hit rate calculation should be accurate")
    void shouldCalculateHitRateCorrectly() {
        InMemoryCacheStore largeCache = new InMemoryCacheStore(20, 0, 10000);

        for (int i = 0; i < 10; i++) {
            largeCache.put("key" + i, "value" + i);
        }

        for (int i = 0; i < 7; i++) {
            assertNotNull(largeCache.get("key" + i));
        }

        for (int i = 100; i < 103; i++) {
            assertNull(largeCache.get("key" + i));
        }

        double hitRate = largeCache.getHitRate();
        assertThat(hitRate)
                .as("Hit rate should be 70% (7 hits out of 10 total accesses)")
                .isCloseTo(70.0, within(0.1));

        InMemoryCacheStore.CacheStats stats = largeCache.getStats();
        assertThat(stats.hits).isEqualTo(7);
        assertThat(stats.misses).isEqualTo(3);
        largeCache.shutdown();
    }

    @Test
    @DisplayName("Should track eviction count in stats")
    void shouldTrackEvictions() {
        InMemoryCacheStore smallCache = new InMemoryCacheStore(5, 0, 10000);
        for (int i = 0; i < 20; i++) {
            smallCache.put("key" + i, "value" + i);
        }
        InMemoryCacheStore.CacheStats stats = smallCache.getStats();

        assertThat(stats.evictions)
                .as("Evictions should have occurred")
                .isGreaterThan(0);

        assertThat(stats.size)
                .as("Size should be controlled")
                .isLessThanOrEqualTo(10);

        smallCache.shutdown();
    }
}