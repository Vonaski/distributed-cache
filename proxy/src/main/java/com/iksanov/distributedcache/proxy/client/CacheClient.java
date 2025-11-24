package com.iksanov.distributedcache.proxy.client;

import com.iksanov.distributedcache.common.cluster.NodeInfo;
import com.iksanov.distributedcache.common.codec.CacheMessageCodec;
import com.iksanov.distributedcache.common.dto.CacheRequest;
import com.iksanov.distributedcache.common.dto.CacheResponse;
import com.iksanov.distributedcache.common.exception.CacheConnectionException;
import com.iksanov.distributedcache.proxy.config.CacheClusterConfig;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.util.AttributeKey;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.*;

@Component
public class CacheClient {

    private static final Logger log = LoggerFactory.getLogger(CacheClient.class);
    private static final AttributeKey<RequestResponseHandler> HANDLER_KEY = AttributeKey.valueOf("handler");
    private final EventLoopGroup eventLoopGroup;
    private final Bootstrap bootstrap;
    private final CacheClusterConfig config;
    private final Map<NodeInfo, Channel> connectionPool = new ConcurrentHashMap<>();

    public CacheClient(CacheClusterConfig config) {
        this.config = config;
        this.eventLoopGroup = new NioEventLoopGroup(config.getConnection().getPoolSize());
        this.bootstrap = createBootstrap();
    }

    private Bootstrap createBootstrap() {
        return new Bootstrap()
                .group(eventLoopGroup)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, config.getConnection().getTimeoutMillis())
                .option(ChannelOption.SO_KEEPALIVE, true)
                .option(ChannelOption.TCP_NODELAY, true);
    }

    public CompletableFuture<CacheResponse> sendRequest(NodeInfo node, CacheRequest request) {
        CompletableFuture<CacheResponse> future = new CompletableFuture<>();

        try {
            Channel channel = getOrCreateChannel(node);
            RequestResponseHandler handler = channel.attr(HANDLER_KEY).get();
            if (handler != null) {
                handler.registerRequest(request.requestId(), future);
            } else {
                future.completeExceptionally(new CacheConnectionException("No handler found for channel"));
                return future;
            }

            channel.writeAndFlush(request).addListener((ChannelFutureListener) writeFuture -> {
                if (!writeFuture.isSuccess()) {
                    future.completeExceptionally(new CacheConnectionException("Failed to write request to " + node.nodeId(), writeFuture.cause()));
                    removeChannel(node);
                }
            });
        } catch (Exception e) {
            future.completeExceptionally(e);
        }
        return future;
    }

    private Channel getOrCreateChannel(NodeInfo node) {
        Channel channel = connectionPool.get(node);
        if (channel != null && channel.isActive()) return channel;
        synchronized (this) {
            channel = connectionPool.get(node);
            if (channel != null && channel.isActive()) return channel;
            channel = createChannel(node);
            connectionPool.put(node, channel);
            channel.closeFuture().addListener(f -> {
                log.info("Connection closed to {}", node.nodeId());
                removeChannel(node);
            });
            return channel;
        }
    }

    private Channel createChannel(NodeInfo node) {
        try {
            ChannelFuture future = bootstrap
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            RequestResponseHandler handler = new RequestResponseHandler();
                            ch.attr(HANDLER_KEY).set(handler);
                            ch.pipeline()
                                .addLast(new LengthFieldBasedFrameDecoder(65536, 0, 4, 0, 4))
                                .addLast(new LengthFieldPrepender(4))
                                .addLast(new CacheMessageCodec())
                                .addLast(handler);
                        }
                    })
                    .connect(node.host(), node.port())
                    .sync();

            if (!future.isSuccess()) throw new CacheConnectionException("Failed to connect to " + node.nodeId(), future.cause());
            log.info("Connected to cache node: {}", node.nodeId());
            return future.channel();
        } catch (Exception e) {
            throw new CacheConnectionException("Failed to create channel to " + node.nodeId(), e);
        }
    }

    private void removeChannel(NodeInfo node) {
        Channel channel = connectionPool.remove(node);
        if (channel != null && channel.isActive()) channel.close();
    }

    @PreDestroy
    public void shutdown() {
        log.info("Shutting down CacheClient...");
        connectionPool.values().forEach(channel -> {if (channel.isActive()) channel.close();});
        eventLoopGroup.shutdownGracefully();
        log.info("CacheClient shutdown complete");
    }

    private static class RequestResponseHandler extends SimpleChannelInboundHandler<CacheResponse> {

        private final Map<String, CompletableFuture<CacheResponse>> pendingRequests = new ConcurrentHashMap<>();

        public void registerRequest(String requestId, CompletableFuture<CacheResponse> future) {
            pendingRequests.put(requestId, future);
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, CacheResponse response) {
            CompletableFuture<CacheResponse> future = pendingRequests.remove(response.requestId());
            if (future != null) {
                future.complete(response);
            } else {
                log.warn("Received response for unknown requestId: {}", response.requestId());
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            log.error("Channel exception: {}", cause.getMessage(), cause);
            pendingRequests.values().forEach(f -> {if (!f.isDone()) f.completeExceptionally(cause);});
            pendingRequests.clear();
            ctx.close();
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            pendingRequests.values().forEach(f -> {if (!f.isDone()) f.completeExceptionally(new CacheConnectionException("Channel closed"));});
            pendingRequests.clear();
            ctx.fireChannelInactive();
        }
    }
}