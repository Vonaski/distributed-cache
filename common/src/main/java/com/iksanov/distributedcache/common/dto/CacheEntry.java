package com.iksanov.distributedcache.common.dto;

public record CacheEntry(String key, String value) {
    public CacheEntry {
        if (key == null || key.isBlank()) throw new IllegalArgumentException("key cannot be null or blank");
    }
}
