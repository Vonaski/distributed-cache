package com.iksanov.distributedcache.node.core;

import com.iksanov.distributedcache.common.exception.CacheException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Thread-safe in-memory cache store with TTL + LRU.
 * - Uses ConcurrentHashMap for storage.
 * - Uses ConcurrentLinkedDeque of CacheEntry instances for access order.
 * - Eviction is handled by a single-threaded evictor executor (no CompletableFuture).
 * - Cleanup of expired entries is scheduled.
 * <p>
 * Important invariants:
 * - store (ConcurrentHashMap) is the source of truth for which CacheEntry is current.
 * - accessOrder holds CacheEntry instances; evict/cleanup remove by instance using store.remove(key, entry).
 */
public class InMemoryCacheStore implements CacheStore {

    private static final Logger log = LoggerFactory.getLogger(InMemoryCacheStore.class);
    private final ConcurrentMap<String, CacheEntry> store = new ConcurrentHashMap<>();
    private final Deque<CacheEntry> accessOrder = new ConcurrentLinkedDeque<>();
    private final AtomicInteger currentSize = new AtomicInteger(0);
    private final int maxSize;
    private final long defaultTtlMillis;
    private final ScheduledExecutorService cleaner;
    private final ExecutorService evictor;
    private final AtomicBoolean evictionInProgress = new AtomicBoolean(false);

    public InMemoryCacheStore(int maxSize, long defaultTtlMillis, long cleanupIntervalMillis) {
        this(maxSize, defaultTtlMillis, cleanupIntervalMillis, null, null);
    }

    public InMemoryCacheStore(int maxSize, long defaultTtlMillis, long cleanupIntervalMillis, ScheduledExecutorService cleanerExecutor, ExecutorService evictorExecutor) {
        if (maxSize <= 0) throw new CacheException("maxSize must be > 0");
        if (cleanupIntervalMillis <= 0) throw new CacheException("cleanupIntervalMillis must be > 0");

        this.maxSize = maxSize;
        this.defaultTtlMillis = defaultTtlMillis;
        this.cleaner = cleanerExecutor != null ? cleanerExecutor : buildSingleThreadScheduler("cache-cleaner");
        this.evictor = evictorExecutor != null ? evictorExecutor : buildSingleThreadExecutor("cache-evictor");
        this.cleaner.scheduleAtFixedRate(this::cleanupExpiredEntries, cleanupIntervalMillis, cleanupIntervalMillis, TimeUnit.MILLISECONDS);
        this.cleaner.scheduleAtFixedRate(this::compactAccessOrder, cleanupIntervalMillis * 6, cleanupIntervalMillis * 6, TimeUnit.MILLISECONDS);
        log.info("InMemoryCacheStore initialized: maxSize={}, defaultTTL={}ms, cleanupInterval={}ms", maxSize, defaultTtlMillis, cleanupIntervalMillis);
    }

    private static ScheduledExecutorService buildSingleThreadScheduler(String name) {
        ThreadFactory factory = runnable -> {
            Thread t = new Thread(runnable, name);
            t.setDaemon(true);
            return t;
        };
        return Executors.newSingleThreadScheduledExecutor(factory);
    }


    private static ExecutorService buildSingleThreadExecutor(String name) {
        ThreadFactory factory = runnable -> {
            Thread t = new Thread(runnable, name);
            t.setDaemon(true);
            return t;
        };
        return Executors.newSingleThreadExecutor(factory);
    }

    @Override
    public String get(String key) {
        Objects.requireNonNull(key, "key");
        CacheEntry entry = store.get(key);
        if (entry == null) {
            log.debug("Cache MISS for key={}", key);
            return null;
        }
        if (entry.isExpired()) {
            removeInternal(key, entry);
            log.debug("Cache EXPIRED for key={}", key);
            return null;
        }
        touchEntry(entry);
        log.debug("Cache HIT for key={}", key);
        return entry.value();
    }

    @Override
    public void put(String key, String value) {
        Objects.requireNonNull(key, "key");
        long expireAt = defaultTtlMillis > 0
                ? System.currentTimeMillis() + defaultTtlMillis
                : -1;

        CacheEntry newEntry = new CacheEntry(key, value, expireAt);

        store.compute(key, (_, old) -> {
            if (old == null) {
                currentSize.incrementAndGet();
                log.debug("Put new entry key={} (expireAt={})", key, expireAt > 0 ? expireAt : "∞");
            }
            accessOrder.addLast(newEntry);
            return newEntry;
        });

        if (currentSize.get() > maxSize && evictionInProgress.compareAndSet(false, true)) {
            evictor.submit(() -> {
                try {
                    evictUntilSizeWithinLimit();
                } finally {
                    evictionInProgress.set(false);
                }
            });
        }
        if (log.isInfoEnabled()) log.info("Cache size after put: {}", currentSize.get());
    }

    @Override
    public void delete(String key) {
        Objects.requireNonNull(key, "key");
        CacheEntry removed = store.remove(key);
        if (removed != null) {
            currentSize.decrementAndGet();
            log.debug("Deleted key={}", key);
        }
    }

    @Override
    public void clear() {
        store.clear();
        accessOrder.clear();
        currentSize.set(0);
        log.info("Cache cleared manually");
    }

    @Override
    public int size() {
        return currentSize.get();
    }

    public void shutdown() {
        cleaner.shutdownNow();
        evictor.shutdownNow();
        try {
            if (!cleaner.awaitTermination(200, TimeUnit.MILLISECONDS)) {
                log.warn("Cleaner did not terminate promptly");
            }
            if (!evictor.awaitTermination(500, TimeUnit.MILLISECONDS)) {
                log.warn("Evictor did not terminate promptly");
            }
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }
        log.info("Cache cleaner and evictor shutdown");
    }

    private void touchEntry(CacheEntry entry) {
        accessOrder.addLast(entry);
    }

    private void evictUntilSizeWithinLimit() {
        try {
            while (currentSize.get() > maxSize) {
                CacheEntry candidate = accessOrder.pollFirst();
                if (candidate == null) break;

                CacheEntry current = store.get(candidate.key());
                if (current != candidate) continue;

                boolean removed = store.remove(candidate.key(), candidate);
                if (removed) {
                    currentSize.decrementAndGet();
                    log.warn("Evicted key={} due to LRU policy (maxSize={})", candidate.key(), maxSize);
                }
            }
        } catch (Throwable t) {
            log.error("Error during eviction", t);
        }
    }

    private void cleanupExpiredEntries() {
        try {
            List<CacheEntry> toRemove = new ArrayList<>();
            for (ConcurrentHashMap.Entry<String, CacheEntry> me : store.entrySet()) {
                CacheEntry e = me.getValue();
                if (e != null && e.isExpired()) toRemove.add(e);
            }

            int removedCount = 0;
            for (CacheEntry e : toRemove) {
                boolean removed = store.remove(e.key(), e);
                if (removed) {
                    currentSize.decrementAndGet();
                    removedCount++;
                }
            }
            if (removedCount > 0) log.info("Cleaned up {} expired entries (current size={})", removedCount, store.size());
        } catch (Throwable t) {
            log.error("Error during cleanupExpiredEntries", t);
        }
    }

    private void compactAccessOrder() {
        int before = accessOrder.size();
        int skipped = 0;
        for (Iterator<CacheEntry> it = accessOrder.iterator(); it.hasNext();) {
            CacheEntry e = it.next();
            CacheEntry actual = store.get(e.key());
            if (actual != e) {
                it.remove();
                skipped++;
            }
        }
        if (skipped > 0) log.debug("Compacted LRU queue: removed {} stale entries ({} -> {})", skipped, before, accessOrder.size());
    }

    private void removeInternal(String key, CacheEntry entry) {
        if (store.remove(key, entry)) {
            currentSize.decrementAndGet();
        }
    }
}
