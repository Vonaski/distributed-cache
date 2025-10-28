package com.iksanov.distributedcache.node.net;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.atomic.AtomicLong;

/**
 * ChannelLifecycleHandler tracks the lifecycle of client connections.
 * <p>
 * Responsibilities:
 *  - Log connection and disconnection events
 *  - Maintain the number of currently active connections
 *  - Provide a simple way to expose metrics in the future
 */
@ChannelHandler.Sharable
public class ChannelLifecycleHandler extends ChannelInboundHandlerAdapter {

    private static final Logger log = LoggerFactory.getLogger(ChannelLifecycleHandler.class);
    private final AtomicLong activeConnections = new AtomicLong(0);

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        long count = activeConnections.incrementAndGet();
        log.info("Client connected: {} (active connections = {})", ctx.channel().remoteAddress(), count);
        ctx.fireChannelActive();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        long count = activeConnections.updateAndGet(prev -> Math.max(0, prev - 1));
        log.info("Client disconnected: {} (active connections = {})", ctx.channel().remoteAddress(), count);
        ctx.fireChannelInactive();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("Unhandled exception from client {}: {}", ctx.channel().remoteAddress(), cause.getMessage(), cause);
        ctx.close();
    }

    public long getActiveConnections() {
        return activeConnections.get();
    }
}
