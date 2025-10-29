package com.iksanov.distributedcache.node.core;

public class CacheEntry {
    private final String key;
    private volatile String value;
    private final long expireAt;
    private final long timestamp;

    public CacheEntry(String key, String value, long expireAt) {
        this(key, value, expireAt, System.currentTimeMillis());
    }

    public CacheEntry(String key, String value, long expireAt, long timestamp) {
        this.key = key;
        this.value = value;
        this.expireAt = expireAt;
        this.timestamp = timestamp;
    }

    public String key() { return key; }
    public String value() { return value; }
    public long expireAt() { return expireAt; }
    public long timestamp() { return timestamp; }

    public boolean isExpired() {
        return expireAt > 0 && System.currentTimeMillis() >= expireAt;
    }

    public void setValue(String newValue) {
        this.value = newValue;
    }
}
