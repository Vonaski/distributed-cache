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
        String origin
) implements Serializable {

    public ReplicationTask {
        Objects.requireNonNull(key, "key");
        if (key.isBlank()) throw new IllegalArgumentException("key cannot be blank");
        Objects.requireNonNull(operation, "operation");
        if (timestamp <= 0) throw new IllegalArgumentException("timestamp must be positive");
    }

    public static ReplicationTask ofSet(String key, String value, String origin) {
        return new ReplicationTask(key, value, Operation.SET, System.currentTimeMillis(), origin);
    }

    public static ReplicationTask ofDelete(String key, String origin) {
        return new ReplicationTask(key, null, Operation.DELETE, System.currentTimeMillis(), origin);
    }

    public enum Operation {
        SET,
        DELETE
    }
}
