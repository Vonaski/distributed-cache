package com.iksanov.distributedcache.node.replication;

import com.iksanov.distributedcache.node.core.CacheStore;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * ReplicationReceiver — Netty server which accepts replication tasks from peers
 * and applies them to local CacheStore.
 * <p>
 * Improvements:
 * - Self-origin check to prevent replication loops
 * - NodeId parameter for origin validation
 */
public final class ReplicationReceiver {

    private static final Logger log = LoggerFactory.getLogger(ReplicationReceiver.class);
    private static final int DEFAULT_MAX_FRAME_LENGTH = 1024 * 1024;
    private final String host;
    private final int port;
    private final CacheStore store;
    private final int maxFrameLength;
    private final String nodeId;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private Channel serverChannel;
    private volatile boolean running = false;

    public ReplicationReceiver(String host, int port, CacheStore store) {
        this(host, port, store, DEFAULT_MAX_FRAME_LENGTH, null);
    }

    public ReplicationReceiver(String host, int port, CacheStore store, int maxFrameLength) {
        this(host, port, store, maxFrameLength, null);
    }

    public ReplicationReceiver(String host, int port, CacheStore store, int maxFrameLength, String nodeId) {
        this.host = Objects.requireNonNull(host, "host");
        this.port = port;
        this.store = Objects.requireNonNull(store, "store");
        this.maxFrameLength = maxFrameLength > 0 ? maxFrameLength : DEFAULT_MAX_FRAME_LENGTH;
        this.nodeId = nodeId;
    }

    public void start() {
        if (running) {
            log.warn("ReplicationReceiver is already running");
            return;
        }

        bossGroup = new NioEventLoopGroup(1);
        workerGroup = new NioEventLoopGroup();

        try {
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .handler(new LoggingHandler(LogLevel.INFO))
                    .childHandler(new ChannelInitializer<>() {
                        @Override
                        protected void initChannel(Channel ch) {
                            ChannelPipeline p = ch.pipeline();
                            p.addLast(new LengthFieldBasedFrameDecoder(maxFrameLength, 0, 4, 0, 4));
                            p.addLast(new ReplicationMessageCodec());
                            p.addLast(new SimpleChannelInboundHandler<ReplicationTask>() {
                                @Override
                                protected void channelRead0(ChannelHandlerContext ctx, ReplicationTask task) {
                                    log.debug("Receiver on {}:{} got replication task {} -> {}",
                                            host, port, task.key(), task.operation());
                                    try {
                                        applyTask(task);
                                    } catch (Exception e) {
                                        log.error("Failed to apply replication task: {}", e.getMessage(), e);
                                    }
                                }
                                @Override
                                public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                                    log.error("Replication pipeline exception from {}: {}",
                                            ctx.channel().remoteAddress(), cause.getMessage(), cause);
                                    ctx.close();
                                }
                            });
                        }
                    });

            InetSocketAddress address = new InetSocketAddress(host, port);
            ChannelFuture future = bootstrap.bind(address).syncUninterruptibly();
            if (future.isSuccess()) {
                serverChannel = future.channel();
                running = true;
                log.info("ReplicationReceiver started on {}:{}", host, port);
            } else {
                log.error("Failed to bind ReplicationReceiver on {}:{}", host, port, future.cause());
                shutdownEventLoopGroupsQuietly();
            }
        } catch (Throwable t) {
            log.error("Unexpected error while starting ReplicationReceiver", t);
            shutdownEventLoopGroupsQuietly();
            throw new RuntimeException("ReplicationReceiver startup failed", t);
        }
    }

    public void stop() {
        if (!running) {
            log.warn("ReplicationReceiver is not running");
            return;
        }

        log.info("Stopping ReplicationReceiver...");
        try {
            if (serverChannel != null) {
                serverChannel.close().syncUninterruptibly();
            }
        } finally {
            shutdownEventLoopGroups();
            running = false;
            log.info("ReplicationReceiver stopped successfully");
        }
    }

    private void shutdownEventLoopGroups() {
        try {
            if (workerGroup != null) {
                workerGroup.shutdownGracefully().await(5, TimeUnit.SECONDS);
            }
            if (bossGroup != null) {
                bossGroup.shutdownGracefully().await(5, TimeUnit.SECONDS);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void shutdownEventLoopGroupsQuietly() {
        try {
            if (workerGroup != null) workerGroup.shutdownGracefully();
        } catch (Throwable ignored) {
        }
        try {
            if (bossGroup != null) bossGroup.shutdownGracefully();
        } catch (Throwable ignored) {
        }
    }

    public boolean isRunning() {
        return running;
    }

    public void applyTask(ReplicationTask task) {
        if (task == null) return;

        if (nodeId != null && nodeId.equals(task.origin())) {
            log.debug("Ignoring self-origin replication task for key={} from origin={}",
                    task.key(), task.origin());
            return;
        }

        try {
            switch (task.operation()) {
                case SET -> store.put(task.key(), task.value());
                case DELETE -> store.delete(task.key());
                default -> log.warn("Unknown replication operation: {}", task.operation());
            }
        } catch (Exception e) {
            log.error("Failed to apply replication task to store: key={}, operation={}, error={}",
                    task.key(), task.operation(), e.getMessage(), e);
        }
    }
}