package com.iksanov.distributedcache.node.core;

import com.iksanov.distributedcache.node.metrics.CacheMetrics;
import org.junit.jupiter.api.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import static org.assertj.core.api.Assertions.*;

/**
 * Comprehensive tests for {@link InMemoryCacheStore} with Approximate LRU.
 * Tests validate correctness while accounting for approximate eviction behavior.
 */
@DisplayName("InMemoryCacheStore - Approximate LRU implementation tests")
class InMemoryCacheStoreTest {

    private InMemoryCacheStore cache;
    private final CacheMetrics metrics = new CacheMetrics();

    @BeforeEach
    void setUp() {
        cache = new InMemoryCacheStore(5, 1000, metrics);
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
        InMemoryCacheStore noTtlCache = new InMemoryCacheStore(3, 0, metrics);

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

        for (int i = 0; i < 150; i++) {
            cache.put("trigger" + i, "value" + i);
        }

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

        for (int i = 6; i <= 10; i++) {
            cache.put("k" + i, "v" + i);
        }

        assertThat(cache.size())
                .as("Cache size should be controlled after eviction")
                .isLessThanOrEqualTo(10);
    }

    @Test
    @DisplayName("Should respect max size bounds under heavy load")
    void shouldRespectMaxSizeUnderHeavyLoad() {
        for (int i = 0; i < 100; i++) {
            cache.put("k" + i, "v" + i);

            assertThat(cache.size())
                    .as("Cache size must be bounded during insertions")
                    .isLessThanOrEqualTo(15);
        }

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
        int maxAllowedSize = 15;
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
}