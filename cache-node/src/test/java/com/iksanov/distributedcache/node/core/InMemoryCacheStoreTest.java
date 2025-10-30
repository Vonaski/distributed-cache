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
 *  - Strict size enforcement
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
    @DisplayName("put() on existing key should update value and refresh position")
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
    @DisplayName("Entries should expire after TTL")
    void shouldExpireEntriesAfterTTL() throws InterruptedException {
        cache.put("temp", "data");
        assertThat(cache.get("temp")).isEqualTo("data");

        Thread.sleep(1200);

        assertThat(cache.get("temp"))
                .as("Expired entry should be removed on access")
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
    @DisplayName("Cleanup thread should remove expired entries automatically")
    void shouldRemoveExpiredEntriesDuringCleanup() throws InterruptedException {
        cache.put("auto", "cleanup");

        // Wait for TTL expiration
        Thread.sleep(1100);

        // Wait for cleanup cycle
        Thread.sleep(600);

        assertThat(cache.size())
                .as("Expired entries should be cleaned up")
                .isEqualTo(0);
    }

    @Test
    @DisplayName("Should evict least recently used entry when max size is exceeded")
    void shouldEvictLeastRecentlyUsedEntry() {
        // Fill cache to max size
        for (int i = 1; i <= 5; i++) {
            cache.put("k" + i, "v" + i);
        }
        assertThat(cache.size()).isEqualTo(5);

        // Add one more - should trigger immediate eviction
        cache.put("k6", "v6");

        // Size should never exceed maxSize with synchronous eviction
        assertThat(cache.size()).isEqualTo(5);

        // First key should be evicted (LRU)
        assertThat(cache.get("k1"))
                .as("The oldest key (k1) should be evicted by LRU")
                .isNull();

        // Most recent keys should remain
        assertThat(cache.get("k6")).isEqualTo("v6");
    }

    @Test
    @DisplayName("get() should refresh entry position for LRU eviction order")
    void shouldRefreshEntryOnGet() {
        // Fill cache
        for (int i = 1; i <= 5; i++) {
            cache.put("k" + i, "v" + i);
        }

        // Access k1 to make it recently used
        assertThat(cache.get("k1")).isEqualTo("v1");

        // Add new entry
        cache.put("k6", "v6");

        // k1 should NOT be evicted since we just accessed it
        assertThat(cache.get("k1"))
                .as("Recently accessed key should not be evicted")
                .isEqualTo("v1");

        // k2 should be evicted instead (now the LRU)
        assertThat(cache.get("k2"))
                .as("k2 should be evicted as the new LRU")
                .isNull();
    }

    @Test
    @DisplayName("Should strictly enforce max size (never exceed)")
    void shouldStrictlyEnforceMaxSize() {
        // Rapid insertions
        for (int i = 0; i < 100; i++) {
            cache.put("k" + i, "v" + i);

            // Size should NEVER exceed maxSize with synchronous eviction
            assertThat(cache.size())
                    .as("Cache size must never exceed maxSize")
                    .isLessThanOrEqualTo(5);
        }

        assertThat(cache.size()).isEqualTo(5);
    }

    @Test
    @DisplayName("Should handle concurrent access correctly")
    void shouldHandleConcurrentAccess() throws InterruptedException {
        int threads = 10;
        int iterations = 200;
        AtomicInteger successfulGets = new AtomicInteger();
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threads);

        for (int t = 0; t < threads; t++) {
            final int threadId = t;
            new Thread(() -> {
                try {
                    startLatch.await();

                    for (int i = 0; i < iterations; i++) {
                        String key = "key-" + (i % 20);

                        if (threadId < 4) {
                            // Writers
                            cache.put(key, "val-" + threadId + "-" + i);
                        } else if (threadId < 8) {
                            // Readers
                            String val = cache.get(key);
                            if (val != null) successfulGets.incrementAndGet();
                        } else {
                            // Deleters
                            if (i % 2 == 0) {
                                cache.delete(key);
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
                .as("Cache size should be within bounds")
                .isLessThanOrEqualTo(5);

        assertThat(successfulGets.get())
                .as("Some reads should have succeeded")
                .isGreaterThan(0);
    }

    @Test
    @DisplayName("Concurrent writes should maintain size invariant")
    void shouldMaintainSizeInvariantUnderConcurrentWrites() throws InterruptedException {
        int threads = 8;
        int maxSize = 5;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threads);

        for (int t = 0; t < threads; t++) {
            new Thread(() -> {
                try {
                    startLatch.await();
                    for (int i = 0; i < 100; i++) {
                        cache.put("key-" + ThreadLocalRandom.current().nextInt(50), "val-" + i);

                        // Check invariant after each put
                        int currentSize = cache.size();
                        if (currentSize > maxSize) {
                            fail("Size invariant violated: " + currentSize + " > " + maxSize);
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
                .as("Final size must be within limit")
                .isLessThanOrEqualTo(maxSize);
    }

    @Test
    @DisplayName("shutdown() should stop cleanup thread gracefully")
    void shouldShutdownGracefully() {
        assertThatCode(() -> cache.shutdown())
                .doesNotThrowAnyException();

        // After shutdown, cache should be cleared
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
}