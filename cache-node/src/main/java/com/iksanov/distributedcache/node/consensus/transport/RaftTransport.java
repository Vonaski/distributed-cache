package com.iksanov.distributedcache.node.consensus.transport;

import java.util.function.Consumer;

/**
 * Transport interface for Raft RPCs.
 * <p>
 * Implementations (like NettyRaftTransport) handle connection management,
 * serialization (via RaftMessageCodec), and asynchronous delivery.
 */
public interface RaftTransport {

    void start() throws Exception;
    void shutdown();
    void send(TransportMessage message);
    void registerHandler(MessageType type, Consumer<TransportMessage> handler);
}
