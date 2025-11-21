package com.iksanov.distributedcache.node.core;

import com.iksanov.distributedcache.common.exception.CacheException;
import com.iksanov.distributedcache.node.metrics.CacheMetrics;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
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
    private final CacheMetrics metrics;
    private static final int SAMPLE_SIZE = 8;
    private static final int EVICTION_BATCH = 5;
    private final AtomicInteger putCounter = new AtomicInteger();
    private static final int LAZY_CLEANUP_INTERVAL = 100;

    public InMemoryCacheStore(int maxSize, long defaultTtlMillis, long cleanupIntervalMillis) {
        this(maxSize, defaultTtlMillis, cleanupIntervalMillis, new CacheMetrics());
    }

    public InMemoryCacheStore(int maxSize, long defaultTtlMillis, long cleanupIntervalMillis, CacheMetrics metrics) {
        if (maxSize <= 0) throw new CacheException("maxSize must be > 0");
        this.maxSize = maxSize;
        this.defaultTtlMillis = defaultTtlMillis;
        this.metrics = metrics;
        log.info("InMemoryCacheStore initialized: maxSize={}, defaultTTL={}ms, eviction=ApproximateLRU", maxSize, defaultTtlMillis);
    }

    @Override
    public String get(String key) {
        Objects.requireNonNull(key, "key");
        Timer.Sample sample = metrics.startGetTimer();

        try {
            CacheEntry entry = store.get(key);
            if (entry == null) {
                metrics.recordMiss();
                return null;
            }

            if (entry.isExpired()) {
                store.remove(key, entry);
                metrics.recordMiss();
                return null;
            }

            entry.recordAccess();
            metrics.recordHit();
            return entry.value;
        } finally {
            metrics.stopGetTimer(sample);
            metrics.updateSize(store.size());
        }
    }

    @Override
    public void put(String key, String value) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(value, "value");

        Timer.Sample sample = metrics.startPutTimer();

        try {
            long expireAt = defaultTtlMillis > 0 ? System.currentTimeMillis() + defaultTtlMillis : -1;
            CacheEntry newEntry = new CacheEntry(key, value, expireAt);
            CacheEntry previous = store.put(key, newEntry);

            if (previous == null) {
                if (store.size() > maxSize) {
                    if (evictionLock.tryLock()) {
                        try {
                            if (store.size() > maxSize) evictEntries();
                        } finally {
                            evictionLock.unlock();
                        }
                    }
                }

                if (store.size() > (maxSize * 12 / 10)) {
                    evictionLock.lock();
                    try {
                        if (store.size() > maxSize) {
                            log.warn("Hard limit triggered: currentSize={}, maxSize={}, evicting aggressively", store.size(), maxSize);
                            evictEntriesAggressively();
                        }
                    } finally {
                        evictionLock.unlock();
                    }
                }
            }
            if (putCounter.incrementAndGet() % LAZY_CLEANUP_INTERVAL == 0) lazyCleanupExpired();
        } finally {
            metrics.stopPutTimer(sample);
            metrics.updateSize(store.size());
        }
    }

    @Override
    public void delete(String key) {
        Objects.requireNonNull(key, "key");
        store.remove(key);
        metrics.updateSize(store.size());
    }

    @Override
    public void clear() {
        store.clear();
        metrics.updateSize(0);
        log.info("Cache cleared");
    }

    @Override
    public int size() {
        return store.size();
    }

    public void shutdown() {
        clear();
        log.info("Cache shutdown");
    }

    private void evictEntries() {
        int toEvict = Math.max(1, store.size() - maxSize + EVICTION_BATCH);

        for (int i = 0; i < toEvict && store.size() > maxSize; i++) {
            CacheEntry victim = findEvictionCandidate();
            if (victim != null && store.remove(victim.key, victim)) {
                metrics.recordEviction();
            }
        }
    }

    private void evictEntriesAggressively() {
        int currentOverflow = store.size() - maxSize;
        int toEvict = Math.max(EVICTION_BATCH * 2, currentOverflow + EVICTION_BATCH);

        int evicted = 0;
        for (int i = 0; i < toEvict && store.size() > maxSize; i++) {
            CacheEntry victim = findEvictionCandidate();
            if (victim != null && store.remove(victim.key, victim)) {
                metrics.recordEviction();
                evicted++;
            }
        }
        if (evicted > 0) log.info("Aggressive eviction completed: removed {} entries, size now {}/{}", evicted, store.size(), maxSize);
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
                if (store.remove(entry.getKey(), cacheEntry)) cleaned++;
            }
        }
        if (cleaned > 0) log.debug("Lazy cleanup removed {} expired entries", cleaned);
    }
}