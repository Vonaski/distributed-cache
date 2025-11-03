package com.iksanov.distributedcache.node.replication;

import java.io.Serializable;
import java.util.Objects;

/**
 * Represents a single replication event that should be propagated
 * from a master node to its replicas.
 * <p>
 * This record is compact, immutable, and codec-friendly.
 * Serialization is handled via ReplicationMessageCodec.
 */
public record ReplicationTask(
        String key,
        String value,
        Operation operation,
        long timestamp,
        String origin,
        long sequence
) implements Serializable {

    public ReplicationTask {
        Objects.requireNonNull(key, "key");
        if (key.isBlank()) throw new IllegalArgumentException("key cannot be blank");
        Objects.requireNonNull(operation, "operation");
        if (timestamp <= 0) throw new IllegalArgumentException("timestamp must be positive");
        if (sequence < 0) throw new IllegalArgumentException("sequence must be >= 0");
    }

    public ReplicationTask(String key, String value, Operation operation, long timestamp, String origin) {
        this(key, value, operation, timestamp, origin, 0L);
    }

    public static ReplicationTask ofSet(String key, String value, String origin, long sequence) {
        return new ReplicationTask(key, value, Operation.SET, System.currentTimeMillis(), origin, sequence);
    }

    public static ReplicationTask ofDelete(String key, String origin, long sequence) {
        return new ReplicationTask(key, null, Operation.DELETE, System.currentTimeMillis(), origin, sequence);
    }

    public static ReplicationTask ofSet(String key, String value, String origin) {
        return new ReplicationTask(key, value, Operation.SET,
                System.currentTimeMillis(), origin, 0L);
    }

    public static ReplicationTask ofDelete(String key, String origin) {
        return new ReplicationTask(key, null, Operation.DELETE,
                System.currentTimeMillis(), origin, 0L);
    }

    public enum Operation {
        SET,
        DELETE
    }
}
