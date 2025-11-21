package com.iksanov.distributedcache.common.dto;

/**
 * CacheResponse with optional metadata for admin commands.
 * <p>
 * For regular commands (GET/SET/DELETE):
 *   - metadata is null
 * <p>
 * For STATUS command:
 *   - metadata contains "role:MASTER,epoch:123"
 * <p>
 * For PROMOTE command:
 *   - metadata contains "role:MASTER,epoch:124" (after promotion)
 */
public record CacheResponse(String requestId, String value, Status status, String errorMessage, String metadata) {
    public enum Status {
        OK, NOT_FOUND, ERROR
    }
    public CacheResponse(String requestId, String value, Status status, String errorMessage) {
        this(requestId, value, status, errorMessage, null);
    }

    public static CacheResponse ok(String id, String value) {
        return new CacheResponse(id, value, Status.OK, null, null);
    }

    public static CacheResponse notFound(String id) {
        return new CacheResponse(id, null, Status.NOT_FOUND, null, null);
    }

    public static CacheResponse error(String id, String msg) {
        return new CacheResponse(id, null, Status.ERROR, msg, null);
    }

    public static CacheResponse okWithMetadata(String id, String metadata) {
        return new CacheResponse(id, null, Status.OK, null, metadata);
    }
}
