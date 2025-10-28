package com.iksanov.distributedcache.node.core;

public interface CacheStore {
    String get(String key);
    void put(String key, String value);
    void delete(String key);
    int size();
    void clear();
}
