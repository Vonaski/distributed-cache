package com.iksanov.distributedcache.common.exception;

public class CacheConnectionException extends CacheException {
    public CacheConnectionException(String message) {
        super(message);
    }
    public CacheConnectionException(String message, Throwable cause) {
        super(message, cause);
    }
}
