package com.iksanov.distributedcache.node.net;

import com.iksanov.distributedcache.common.codec.CacheMessageCodec;
import com.iksanov.distributedcache.node.core.CacheStore;
import com.iksanov.distributedcache.node.replication.ReplicationManager;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;

/**
 * NetServerInitializer is responsible for configuring the Netty pipeline
 * for each newly accepted client connection.
 * <p>
 * The initializer defines:
 *  - message framing (decoder/encoder)
 *  - serialization (CacheMessageCodec)
 *  - business logic handler (NetConnectionHandler)
 */
public class NetServerInitializer extends ChannelInitializer<SocketChannel> {

    private final CacheStore store;
    private final int maxFrameLength;
    private final ChannelLifecycleHandler lifecycleHandler;
    private final ReplicationManager replicationManager;

    public NetServerInitializer(CacheStore store, int maxFrameLength) {
        this(store, maxFrameLength, null);
    }

    public NetServerInitializer(CacheStore store, int maxFrameLength, ReplicationManager replicationManager) {
        this.store = store;
        this.maxFrameLength = maxFrameLength;
        this.lifecycleHandler = new ChannelLifecycleHandler();
        this.replicationManager = replicationManager;
    }

    @Override
    protected void initChannel(SocketChannel ch) {
        ChannelPipeline p = ch.pipeline();
        p.addLast(lifecycleHandler);
        p.addLast(new LoggingHandler(LogLevel.DEBUG));
        p.addLast(new LengthFieldBasedFrameDecoder(maxFrameLength, 0, 4, 0, 4));
        p.addLast(new LengthFieldPrepender(4));
        p.addLast(new CacheMessageCodec());
        if (replicationManager != null) {
            p.addLast(new NetConnectionHandler(new RequestProcessor(store, replicationManager)));
        } else {
            p.addLast(new NetConnectionHandler(store));
        }
    }
}
