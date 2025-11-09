package com.iksanov.distributedcache.node.consensus.model;

import java.util.Objects;

/**
 * Operation stored in RaftLogEntry — keep minimal.
 */
public record Command(Type type, String key, String value) {
    public enum Type {SET, DELETE}

    public Command(Type type, String key, String value) {
        this.type = Objects.requireNonNull(type);
        this.key = Objects.requireNonNull(key);
        this.value = value;
    }

    public static Command set(String key, String value) {
        return new Command(Type.SET, key, value);
    }

    public static Command delete(String key) {
        return new Command(Type.DELETE, key, null);
    }

    @Override
    public String toString() {
        return "CacheCommand{" + type + " " + key + (value == null ? "" : "=" + value) + '}';
    }
}
