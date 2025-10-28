package com.iksanov.distributedcache.node.net;

import com.iksanov.distributedcache.node.config.NetServerConfig;
import com.iksanov.distributedcache.node.core.CacheStore;
import com.iksanov.distributedcache.node.replication.ReplicationManager;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * NetServer - Netty TCP server for cache-node.
 * <p>
 * Responsibilities:
 *  - Initializes and manages Netty event loops (boss & worker groups)
 *  - Builds the pipeline: framing -> codec -> business handler
 *  - Handles lifecycle: start(), stop(), graceful shutdown
 */
public final class NetServer {

    private static final Logger log = LoggerFactory.getLogger(NetServer.class);
    private final NetServerConfig config;
    private final CacheStore store;
    private final ReplicationManager replicationManager;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private Channel serverChannel;
    private volatile boolean running = false;

    public NetServer(NetServerConfig config, CacheStore store) {
        this(config, store, null);
    }

    public NetServer(NetServerConfig config, CacheStore store, ReplicationManager replicationManager) {
        this.config = Objects.requireNonNull(config, "config");
        this.store = Objects.requireNonNull(store, "store");
        this.replicationManager = replicationManager;
    }

    /**
     * Starts the Netty server and binds to configured address.
     */
    public void start() {
        if (running) {
            log.warn("NetServer is already running");
            return;
        }

        bossGroup = new NioEventLoopGroup(Math.max(1, config.bossThreads()));
        workerGroup = config.workerThreads() > 0 ? new NioEventLoopGroup(config.workerThreads()) : new NioEventLoopGroup();

        try {
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .option(ChannelOption.SO_BACKLOG, config.backlog())
                    .option(ChannelOption.SO_REUSEADDR, true)
                    .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                    .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                    .handler(new LoggingHandler(LogLevel.INFO))
                    .childHandler(new NetServerInitializer(store, config.maxFrameLength(), replicationManager));

            InetSocketAddress address = new InetSocketAddress(config.host(), config.port());
            bootstrap.bind(address).addListener((ChannelFuture future) -> {
                if (future.isSuccess()) {
                    serverChannel = future.channel();
                    running = true;
                    log.info("NetServer configuration: {}", config);
                    log.info("NetServer started successfully on {}:{}", config.host(), config.port());
                } else {
                    log.error("Failed to bind NetServer on {}:{}", config.host(), config.port(), future.cause());
                    shutdownEventLoopGroupsQuietly();
                }
            });
        } catch (Throwable t) {
            log.error("Unexpected error while starting NetServer", t);
            shutdownEventLoopGroupsQuietly();
            throw new RuntimeException("NetServer startup failed", t);
        }
    }

    /**
     * Stops the server gracefully.
     */
    public void stop() {
        if (!running) {
            log.warn("NetServer is not running");
            return;
        }

        log.info("Stopping NetServer...");
        try {
            if (serverChannel != null) {
                serverChannel.close().syncUninterruptibly();
            }
        } finally {
            shutdownEventLoopGroups();
            running = false;
            log.info("NetServer stopped successfully");
        }
    }

    private void shutdownEventLoopGroups() {
        try {
            if (workerGroup != null) {
                workerGroup.shutdownGracefully(
                                config.shutdownQuietPeriodSeconds(),
                                config.shutdownTimeoutSeconds(),
                                TimeUnit.SECONDS
                        )
                        .await(config.shutdownTimeoutSeconds(), TimeUnit.SECONDS);
            }

            if (bossGroup != null) {
                bossGroup.shutdownGracefully(
                                config.shutdownQuietPeriodSeconds(),
                                config.shutdownTimeoutSeconds(),
                                TimeUnit.SECONDS
                        )
                        .await(config.shutdownTimeoutSeconds(), TimeUnit.SECONDS);
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
}
