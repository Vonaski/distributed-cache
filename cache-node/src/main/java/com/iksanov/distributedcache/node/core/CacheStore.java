package com.iksanov.distributedcache.node.core;

import java.util.Map;

public interface CacheStore {
    String get(String key);
    void put(String key, String value);
    void delete(String key);
    int size();
    void clear();

    // Snapshot support
    Map<String, String> exportData();
    void importData(Map<String, String> data);
}
