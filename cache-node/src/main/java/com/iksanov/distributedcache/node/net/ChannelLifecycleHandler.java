package com.iksanov.distributedcache.node.net;

import com.iksanov.distributedcache.node.metrics.NetMetrics;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private final NetMetrics metrics;

    public ChannelLifecycleHandler(NetMetrics metrics) {
        this.metrics = metrics;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        metrics.incrementConnections();
        log.info("Connection established from {} (active: {})", ctx.channel().remoteAddress(), metrics.getActiveConnections());
        ctx.fireChannelActive();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        metrics.incrementClosedConnections();
        log.info("Connection closed from {} (active: {})", ctx.channel().remoteAddress(), metrics.getActiveConnections());
        ctx.fireChannelInactive();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        metrics.incrementErrors();
        log.error("Unhandled exception from client {}: {}", ctx.channel().remoteAddress(), cause.getMessage(), cause);
        ctx.close();
    }
}
