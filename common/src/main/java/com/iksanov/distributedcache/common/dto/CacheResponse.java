package com.iksanov.distributedcache.common.dto;

public record CacheResponse(String requestId, String value, Status status, String errorMessage) {
    public enum Status {
        OK, NOT_FOUND, ERROR
    }

    public static CacheResponse ok(String id, String value) {
        return new CacheResponse(id, value, Status.OK, null);
    }

    public static CacheResponse notFound(String id) {
        return new CacheResponse(id, null, Status.NOT_FOUND, null);
    }

    public static CacheResponse error(String id, String msg) {
        return new CacheResponse(id, null, Status.ERROR, msg);
    }
}
