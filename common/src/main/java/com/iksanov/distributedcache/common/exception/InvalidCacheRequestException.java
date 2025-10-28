package com.iksanov.distributedcache.common.exception;

public class InvalidCacheRequestException extends CacheException {
    public InvalidCacheRequestException(String message) {
        super(message);
    }
    public InvalidCacheRequestException(String message, Throwable cause) {
        super(message, cause);
    }
}
