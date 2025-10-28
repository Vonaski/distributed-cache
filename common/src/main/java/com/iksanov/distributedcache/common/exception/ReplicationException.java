package com.iksanov.distributedcache.common.exception;

public class ReplicationException extends CacheException {
    public ReplicationException(String message) {
        super(message);
    }
    public ReplicationException(String message, Throwable cause) {
        super(message, cause);
    }
}
