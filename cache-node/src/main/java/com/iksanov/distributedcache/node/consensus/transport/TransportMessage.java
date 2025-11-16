package com.iksanov.distributedcache.node.consensus.transport;

import java.util.Objects;

/**
 * Generic envelope used by transport layer. payload is the RPC object itself.
 */
public record TransportMessage(MessageType type, String from, String to, long correlationId, Object payload) {
    public TransportMessage(MessageType type, String from, String to, long correlationId, Object payload) {
        this.type = Objects.requireNonNull(type);
        this.from = from;
        this.to = to;
        this.correlationId = correlationId;
        this.payload = Objects.requireNonNull(payload, "payload cannot be null");
    }
}
