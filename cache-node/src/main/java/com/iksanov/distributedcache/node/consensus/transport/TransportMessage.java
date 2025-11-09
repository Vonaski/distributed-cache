package com.iksanov.distributedcache.node.consensus.transport;

import java.util.Objects;

/**
 * Generic envelope used by transport layer. payload is bytes (serialized RPC).
 */
public record TransportMessage(MessageType type, String from, String to, long correlationId, byte[] payload) {
    public TransportMessage(MessageType type, String from, String to, long correlationId, byte[] payload) {
        this.type = Objects.requireNonNull(type);
        this.from = from;
        this.to = to;
        this.correlationId = correlationId;
        this.payload = payload == null ? new byte[0] : payload;
    }
}
