package com.iksanov.distributedcache.node.core;

public interface CacheStore {
    String get(String key);
    void put(String key, String value);
    void delete(String key);
    int size();
    void clear();

    default void put(String key, String value, long timestamp) {
        put(key, value);
    }

    default void delete(String key, long timestamp) {
        delete(key);
    }
}
