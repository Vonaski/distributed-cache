package com.iksanov.distributedcache.common.exception;

public class SerializationException extends CacheException {
    public SerializationException(String message) {
        super(message);
    }
    public SerializationException(String message, Throwable cause) {
        super(message, cause);
    }
}
