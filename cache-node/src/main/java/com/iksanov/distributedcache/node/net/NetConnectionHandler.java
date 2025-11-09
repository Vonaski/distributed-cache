package com.iksanov.distributedcache.node.net;

import com.iksanov.distributedcache.common.dto.CacheRequest;
import com.iksanov.distributedcache.common.dto.CacheResponse;
import com.iksanov.distributedcache.node.metrics.NetMetrics;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * NetConnectionHandler is the main Netty handler responsible for:
 *  - receiving decoded {@link CacheRequest} objects from the pipeline,
 *  - delegating processing to {@link RequestProcessor},
 *  - sending {@link CacheResponse} back to the client,
 *  - handling channel lifecycle events and exceptions.
 * <p>
 * This class is stateless and thread-safe when marked as @Sharable.
 * Each inbound request is handled synchronously by the same EventLoop thread.
 */
@ChannelHandler.Sharable
public class NetConnectionHandler extends SimpleChannelInboundHandler<CacheRequest> {
    private static final Logger log = LoggerFactory.getLogger(NetConnectionHandler.class);
    private final RequestProcessor processor;
    private final NetMetrics metrics;

    public NetConnectionHandler(RequestProcessor processor,  NetMetrics metrics) {
        this.processor = processor;
        this.metrics = metrics;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, CacheRequest request) {
        metrics.incrementRequests();
        long start = System.nanoTime();
        try {
            CacheResponse response = processor.process(request);
            long duration = (System.nanoTime() - start) / 1_000_000;
            metrics.recordRequestDuration(duration);
            metrics.incrementResponses();
            log.debug("Request {} processed in {} ms", request.command(), duration);
            ctx.writeAndFlush(response);
        } catch (Exception e) {
            metrics.incrementErrors();
            log.error("Error while processing requestId={} command={}: {}", request.requestId(), request.command(), e.getMessage(), e);
            CacheResponse error = CacheResponse.error(request.requestId(), e.getMessage());
            ctx.writeAndFlush(error);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        metrics.incrementErrors();
        log.error("Unhandled exception in channel {}: {}", ctx.channel().remoteAddress(), cause.getMessage(), cause);
        ctx.close();
    }
}
