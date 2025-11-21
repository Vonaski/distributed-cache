package com.iksanov.distributedcache.common.dto;

public record CacheRequest(String requestId, long timestamp, Command command, String key, String value) {
    public enum Command {
        GET, SET, DELETE,
        STATUS,
        PROMOTE
    }
}
