package com.iksanov.distributedcache.node.metrics;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.*;
import io.netty.util.CharsetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

/**
 * Simple server for exposing Prometheus metrics.
 * Runs on a separate port (default: 8081) from the main cache port.
 */
public class MetricsServer {

    private static final Logger log = LoggerFactory.getLogger(MetricsServer.class);
    private final int port;
    private final CacheMetrics cacheMetrics;
    private final RaftMetrics raftMetrics;
    private final NetMetrics netMetrics;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private Channel serverChannel;

    public MetricsServer(int port, CacheMetrics metrics, RaftMetrics raftMetrics, NetMetrics netMetrics) {
        this.port = port;
        this.cacheMetrics = metrics;
        this.raftMetrics = raftMetrics;
        this.netMetrics = netMetrics;
    }

    public void start() {
        bossGroup = new NioEventLoopGroup(1);
        workerGroup = new NioEventLoopGroup(2);
        try {
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ch.pipeline()
                                    .addLast(new HttpServerCodec())
                                    .addLast(new HttpObjectAggregator(1024 * 1024))
                                    .addLast(new MetricsHandler(cacheMetrics, raftMetrics, netMetrics));
                        }
                    })
                    .option(ChannelOption.SO_BACKLOG, 128)
                    .childOption(ChannelOption.SO_KEEPALIVE, true);
            ChannelFuture future = bootstrap.bind(port).sync();
            serverChannel = future.channel();
            log.info("Metrics server started on port {}", port);
        } catch (Exception e) {
            log.error("Failed to start metrics server", e);
            shutdown();
        }
    }

    public void shutdown() {
        log.info("Shutting down metrics server...");
        if (serverChannel != null) serverChannel.close();
        if (workerGroup != null) workerGroup.shutdownGracefully();
        if (bossGroup != null) bossGroup.shutdownGracefully();
        log.info("Metrics server shut down");
    }

    private static class MetricsHandler extends SimpleChannelInboundHandler<FullHttpRequest> {
        
        private final CacheMetrics cacheMetrics;
        private final RaftMetrics raftMetrics;
        private final NetMetrics netMetrics;

        MetricsHandler(CacheMetrics cacheMetrics,  RaftMetrics raftMetrics, NetMetrics netMetrics) {
            this.cacheMetrics = cacheMetrics;
            this.raftMetrics = raftMetrics;
            this.netMetrics = netMetrics;
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest request) {
            String uri = request.uri();
            if ("/metrics".equals(uri)) {
                handleMetrics(ctx);
            } else if ("/health".equals(uri)) {
                handleHealth(ctx);
            } else {
                send404(ctx);
            }
        }

        private void handleMetrics(ChannelHandlerContext ctx) {
            String metricsData = cacheMetrics.scrape() + "\n"
                    + raftMetrics.scrape() + "\n"
                    + netMetrics.scrape();
            FullHttpResponse response = new DefaultFullHttpResponse(
                    HttpVersion.HTTP_1_1,
                    HttpResponseStatus.OK,
                    Unpooled.copiedBuffer(metricsData, StandardCharsets.UTF_8)
            );
            response.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/plain; version=0.0.4; charset=utf-8");
            response.headers().set(HttpHeaderNames.CONTENT_LENGTH, response.content().readableBytes());
            ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
        }

        private void handleHealth(ChannelHandlerContext ctx) {
            String healthData = "{\"status\":\"UP\"}";
            
            FullHttpResponse response = new DefaultFullHttpResponse(
                    HttpVersion.HTTP_1_1,
                    HttpResponseStatus.OK,
                    Unpooled.copiedBuffer(healthData, StandardCharsets.UTF_8)
            );
            response.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/json");
            response.headers().set(HttpHeaderNames.CONTENT_LENGTH, response.content().readableBytes());
            ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
        }

        private void send404(ChannelHandlerContext ctx) {
            FullHttpResponse response = new DefaultFullHttpResponse(
                    HttpVersion.HTTP_1_1,
                    HttpResponseStatus.NOT_FOUND,
                    Unpooled.copiedBuffer("Not Found", CharsetUtil.UTF_8)
            );
            response.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/plain");
            response.headers().set(HttpHeaderNames.CONTENT_LENGTH, response.content().readableBytes());
            ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            log.error("Error in metrics handler", cause);
            ctx.close();
        }
    }
}