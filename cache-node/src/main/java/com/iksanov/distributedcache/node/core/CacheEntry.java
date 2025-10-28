package com.iksanov.distributedcache.node.core;

public class CacheEntry {
    private final String key;
    private volatile String value;
    private final long expireAt;

    public CacheEntry(String key, String value, long expireAt) {
        this.key = key;
        this.value = value;
        this.expireAt = expireAt;
    }

    public String key() { return key; }
    public String value() { return value; }
    public long expireAt() { return expireAt; }

    public boolean isExpired() {
        return expireAt > 0 && System.currentTimeMillis() >= expireAt;
    }

    public void setValue(String newValue) {
        this.value = newValue;
    }
}
