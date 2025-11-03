package com.iksanov.distributedcache.node.core;

import java.util.concurrent.atomic.AtomicInteger;

public class CacheEntry {
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
