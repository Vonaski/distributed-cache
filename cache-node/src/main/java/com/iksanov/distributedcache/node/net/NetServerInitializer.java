package com.iksanov.distributedcache.node.net;

import com.iksanov.distributedcache.common.codec.CacheMessageCodec;
import com.iksanov.distributedcache.node.config.ApplicationConfig.NodeRole;
import com.iksanov.distributedcache.node.core.CacheStore;
import com.iksanov.distributedcache.node.metrics.NetMetrics;
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
    private final RequestProcessor requestProcessor;
    private final int maxFrameLength;
    private final NetMetrics metrics;

    public NetServerInitializer(RequestProcessor requestProcessor, int maxFrameLength, NetMetrics metrics) {
        this.requestProcessor = requestProcessor;
        this.maxFrameLength = maxFrameLength;
        this.metrics = metrics;
    }

    @Override
    protected void initChannel(SocketChannel ch) {
        ChannelPipeline p = ch.pipeline();
        p.addLast(new ChannelLifecycleHandler(metrics));
        p.addLast(new LoggingHandler(LogLevel.DEBUG));
        p.addLast(new LengthFieldBasedFrameDecoder(maxFrameLength, 0, 4, 0, 4));
        p.addLast(new LengthFieldPrepender(4));
        p.addLast(new CacheMessageCodec());
        p.addLast(new NetConnectionHandler(requestProcessor, metrics));
    }
}
