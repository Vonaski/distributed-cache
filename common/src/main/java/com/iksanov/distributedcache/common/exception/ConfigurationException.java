package com.iksanov.distributedcache.common.exception;

public class ConfigurationException extends CacheException {
    public ConfigurationException(String message) {
        super(message);
    }
    public ConfigurationException(String message, Throwable cause) {
        super(message, cause);
    }
}
