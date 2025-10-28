package com.iksanov.distributedcache.common.exception;

public class StorageAccessException extends CacheException {
    public StorageAccessException(String message) {
        super(message);
    }
    public StorageAccessException(String message, Throwable cause) {
        super(message, cause);
    }
}
