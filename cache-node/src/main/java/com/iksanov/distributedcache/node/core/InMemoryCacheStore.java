package com.iksanov.distributedcache.node.core;

import com.iksanov.distributedcache.common.exception.CacheException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * High-performance thread-safe cache with Approximate LRU eviction.
 * Uses sampling-based eviction similar to Redis for better concurrency.
 */
public class InMemoryCacheStore implements CacheStore {

    private static final Logger log = LoggerFactory.getLogger(InMemoryCacheStore.class);
    private final ConcurrentMap<String, CacheEntry> store = new ConcurrentHashMap<>();
    private final int maxSize;
    private final long defaultTtlMillis;
    private final ReentrantLock evictionLock = new ReentrantLock();
    private final AtomicLong hits = new AtomicLong();
    private final AtomicLong misses = new AtomicLong();
    private final AtomicLong evictions = new AtomicLong();
    private static final int SAMPLE_SIZE = 8;
    private static final int EVICTION_BATCH = 5;
    private final AtomicInteger putCounter = new AtomicInteger();
    private static final int LAZY_CLEANUP_INTERVAL = 100;

    private static class CacheEntry {
        final String key;
        final String value;
        final long expireAt;
        final AtomicInteger accessCount;
        volatile long lastAccessTime;

        CacheEntry(String key, String value, long expireAt) {
            this.key = key;
            this.value = value;
            this.expireAt = expireAt;
            this.accessCount = new AtomicInteger(0);
            this.lastAccessTime = System.currentTimeMillis();
        }

        boolean isExpired() {
            return expireAt > 0 && System.currentTimeMillis() >= expireAt;
        }

        void recordAccess() {
            accessCount.incrementAndGet();
            lastAccessTime = System.currentTimeMillis();
        }

        int evictionScore() {
            long ageSeconds = (System.currentTimeMillis() - lastAccessTime) / 1000;
            int frequency = accessCount.get();
            return (int)(ageSeconds * 2) - frequency;
        }
    }

    public InMemoryCacheStore(int maxSize, long defaultTtlMillis, long cleanupIntervalMillis) {
        if (maxSize <= 0) throw new CacheException("maxSize must be > 0");
        this.maxSize = maxSize;
        this.defaultTtlMillis = defaultTtlMillis;
        log.info("InMemoryCacheStore initialized: maxSize={}, defaultTTL={}ms, eviction=ApproximateLRU", maxSize, defaultTtlMillis);
    }

    @Override
    public String get(String key) {
        Objects.requireNonNull(key, "key");

        CacheEntry entry = store.get(key);
        if (entry == null) {
            misses.incrementAndGet();
            return null;
        }

        if (entry.isExpired()) {
            store.remove(key, entry);
            misses.incrementAndGet();
            return null;
        }

        entry.recordAccess();
        hits.incrementAndGet();
        return entry.value;
    }

    @Override
    public void put(String key, String value) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(value, "value");

        long expireAt = defaultTtlMillis > 0 ? System.currentTimeMillis() + defaultTtlMillis : -1;
        CacheEntry newEntry = new CacheEntry(key, value, expireAt);

        evictionLock.lock();
        try {
            boolean isUpdate = store.containsKey(key);
            store.put(key, newEntry);

            if (!isUpdate && store.size() > maxSize) {
                evictEntries();
            }
        } finally {
            evictionLock.unlock();
        }

        if (putCounter.incrementAndGet() % LAZY_CLEANUP_INTERVAL == 0) {
            lazyCleanupExpired();
        }
    }

    @Override
    public void delete(String key) {
        Objects.requireNonNull(key, "key");
        store.remove(key);
    }

    @Override
    public void clear() {
        store.clear();
        log.info("Cache cleared");
    }

    @Override
    public int size() {
        return store.size();
    }

    public void shutdown() {
        clear();
        log.info("Cache shutdown - Hits: {}, Misses: {}, HitRate: {:.2f}%, Evictions: {}",
                hits.get(), misses.get(), getHitRate(), evictions.get());
    }

    private void evictEntries() {
        int toEvict = Math.max(1, store.size() - maxSize + EVICTION_BATCH);

        for (int i = 0; i < toEvict && store.size() > maxSize; i++) {
            CacheEntry victim = findEvictionCandidate();
            if (victim != null && store.remove(victim.key, victim)) {
                evictions.incrementAndGet();
            }
        }
    }

    private CacheEntry findEvictionCandidate() {
        Collection<CacheEntry> values = store.values();
        if (values.isEmpty()) return null;

        List<CacheEntry> sample = new ArrayList<>(SAMPLE_SIZE);
        Iterator<CacheEntry> iterator = values.iterator();

        int checked = 0;
        while (iterator.hasNext() && checked < SAMPLE_SIZE * 2) {
            CacheEntry entry = iterator.next();
            checked++;
            if (entry.isExpired()) return entry;
            if (sample.size() < SAMPLE_SIZE) sample.add(entry);
        }

        if (sample.isEmpty()) return null;

        CacheEntry victim = sample.getFirst();
        int maxScore = victim.evictionScore();

        for (int i = 1; i < sample.size(); i++) {
            CacheEntry candidate = sample.get(i);
            int score = candidate.evictionScore();
            if (score > maxScore) {
                maxScore = score;
                victim = candidate;
            }
        }
        return victim;
    }

    private void lazyCleanupExpired() {
        int cleaned = 0;
        int maxChecks = Math.min(20, store.size() / 10);

        Iterator<Map.Entry<String, CacheEntry>> iterator = store.entrySet().iterator();
        for (int i = 0; i < maxChecks && iterator.hasNext(); i++) {
            Map.Entry<String, CacheEntry> entry = iterator.next();
            CacheEntry cacheEntry = entry.getValue();

            if (cacheEntry != null && cacheEntry.isExpired()) {
                if (store.remove(entry.getKey(), cacheEntry)) {
                    cleaned++;
                }
            }
        }

        if (cleaned > 0) {
            log.debug("Lazy cleanup removed {} expired entries", cleaned);
        }
    }

    public double getHitRate() {
        long h = hits.get();
        long m = misses.get();
        long total = h + m;
        return total == 0 ? 0.0 : (h * 100.0) / total;
    }

    public CacheStats getStats() {
        return new CacheStats(hits.get(), misses.get(), getHitRate(), evictions.get(), store.size());
    }

    public static class CacheStats {
        public final long hits;
        public final long misses;
        public final double hitRate;
        public final long evictions;
        public final int size;

        CacheStats(long hits, long misses, double hitRate, long evictions, int size) {
            this.hits = hits;
            this.misses = misses;
            this.hitRate = hitRate;
            this.evictions = evictions;
            this.size = size;
        }

        @Override
        public String toString() {
            return String.format("Stats{hits=%d, misses=%d, hitRate=%.2f%%, evictions=%d, size=%d}",
                    hits, misses, hitRate, evictions, size);
        }
    }
}