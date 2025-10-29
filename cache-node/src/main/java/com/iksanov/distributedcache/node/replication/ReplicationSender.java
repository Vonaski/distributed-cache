package com.iksanov.distributedcache.node.replication;

import com.iksanov.distributedcache.common.cluster.NodeInfo;
import com.iksanov.distributedcache.common.cluster.ReplicaManager;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

/**
 * ReplicationSender manages persistent connections to replica nodes.
 *
 * <p>Key features:
 * <ul>
 *   <li>Connection pooling - reuses channels per replica</li>
 *   <li>Automatic reconnection on failure</li>
 *   <li>Thread-safe concurrent operations</li>
 *   <li>Graceful shutdown with resource cleanup</li>
 * </ul>
 */
public class ReplicationSender {

    private static final Logger log = LoggerFactory.getLogger(ReplicationSender.class);
    private static final int MAX_FRAME_LENGTH = 1024 * 1024;
    private final ReplicaManager replicaManager;
    private final EventLoopGroup eventLoopGroup;
    private final Bootstrap bootstrap;
    private final ConcurrentMap<String, Channel> channels = new ConcurrentHashMap<>();
    private volatile boolean shuttingDown = false;

    public ReplicationSender(ReplicaManager replicaManager) {
        this(replicaManager, null);
    }

    public ReplicationSender(ReplicaManager replicaManager, EventLoopGroup injectedGroup) {
        this.replicaManager = Objects.requireNonNull(replicaManager, "replicaManager");
        this.eventLoopGroup = injectedGroup == null ? new NioEventLoopGroup(2) : injectedGroup;

        this.bootstrap = new Bootstrap()
                .group(this.eventLoopGroup)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 5000);
    }

    public void replicate(NodeInfo master, ReplicationTask task) {
        if (shuttingDown) {
            log.debug("Skipping replication during shutdown");
            return;
        }
        Set<NodeInfo> replicas = replicaManager.getReplicas(master);
        if (replicas == null || replicas.isEmpty()) {
            log.debug("No replicas configured for master {}", master.nodeId());
            return;
        }
        log.debug("Replicating {} operation for key={} to {} replicas", task.operation(), task.key(), replicas.size());
        for (NodeInfo replica : replicas) {
            if (replica == null || replica.nodeId().equals(master.nodeId())) continue;
            sendAsync(replica, task);
        }
    }

    private void sendAsync(NodeInfo replica, ReplicationTask task) {
        Channel ch = getOrCreateChannel(replica);
        if (ch != null && ch.isActive()) writeTask(replica, ch, task);
    }

    private Channel getOrCreateChannel(NodeInfo replica) {
        Channel existing = channels.get(replica.nodeId());
        if (existing != null && existing.isActive()) return existing;
        if (existing != null) {
            channels.remove(replica.nodeId(), existing);
            closeQuietly(existing);
        }
        return connectToReplica(replica);
    }

    private Channel connectToReplica(NodeInfo replica) {
        if (shuttingDown) return null;
        try {
            Bootstrap child = bootstrap.clone();
            child.handler(new ChannelInitializer<>() {
                @Override
                protected void initChannel(Channel ch) {
                    ChannelPipeline p = ch.pipeline();
                    p.addLast(new LengthFieldPrepender(4));
                    p.addLast(new LengthFieldBasedFrameDecoder(MAX_FRAME_LENGTH, 0, 4, 0, 4));
                    p.addLast(new ReplicationMessageCodec());
                    p.addLast(new ChannelInboundHandlerAdapter() {
                        @Override
                        public void channelInactive(ChannelHandlerContext ctx) {
                            log.debug("Connection to replica {} closed", replica.nodeId());
                            channels.remove(replica.nodeId(), ctx.channel());
                        }

                        @Override
                        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                            log.debug("Channel error for replica {}: {}", replica.nodeId(), cause.getMessage());
                            channels.remove(replica.nodeId(), ctx.channel());
                            ctx.close();
                        }
                    });
                }
            });

            ChannelFuture connectFuture = child.connect(replica.host(), replica.replicationPort());

            connectFuture.addListener((ChannelFutureListener) future -> {
                if (future.isSuccess()) {
                    Channel newChannel = future.channel();
                    channels.put(replica.nodeId(), newChannel);
                    log.debug("Connected to replica {} at {}:{}",
                            replica.nodeId(), replica.host(), replica.replicationPort());
                } else {
                    log.warn("Failed to connect to replica {}: {}",
                            replica.nodeId(), future.cause().getMessage());
                }
            });

            if (connectFuture.await(1000, TimeUnit.MILLISECONDS)) {
                if (connectFuture.isSuccess()) {
                    return connectFuture.channel();
                }
            }
        } catch (Exception e) {
            log.warn("Exception connecting to replica {}: {}", replica.nodeId(), e.getMessage());
        }
        return null;
    }

    private void writeTask(NodeInfo replica, Channel ch, ReplicationTask task) {
        if (!ch.isActive()) {
            channels.remove(replica.nodeId(), ch);
            return;
        }

        try {
            ChannelFuture writeFuture = ch.writeAndFlush(task);
            if (writeFuture == null) {
                log.warn("writeAndFlush returned null for replica {}", replica.nodeId());
                removeChannel(replica.nodeId(), ch);
                return;
            }

            writeFuture.addListener((ChannelFutureListener) future -> {
                if (future.isSuccess()) {
                    log.trace("Successfully replicated to {}", replica.nodeId());
                } else {
                    log.warn("Write failed for replica {}: {}",
                            replica.nodeId(), future.cause().getMessage());
                    removeChannel(replica.nodeId(), future.channel());
                }
            });
        } catch (Exception e) {
            log.warn("Exception writing to replica {}: {}", replica.nodeId(), e.getMessage());
            removeChannel(replica.nodeId(), ch);
        }
    }

    private void removeChannel(String replicaId, Channel ch) {
        if (channels.remove(replicaId, ch)) {
            closeQuietly(ch);
        }
    }

    private void closeQuietly(Channel ch) {
        if (ch != null) {
            try {
                if (ch.isOpen()) ch.close();
            } catch (Exception ignored) {
                // Best effort
            }
        }
    }

    public void shutdown() {
        log.info("Shutting down ReplicationSender...");
        shuttingDown = true;
        try {
            for (Channel ch : channels.values()) {
                closeQuietly(ch);
            }
            channels.clear();
            eventLoopGroup.shutdownGracefully(1, 2, TimeUnit.SECONDS).awaitUninterruptibly(3, TimeUnit.SECONDS);
            log.info("ReplicationSender shutdown complete");
        } catch (Exception e) {
            log.warn("Error during shutdown: {}", e.getMessage());
        }
    }

    ConcurrentMap<String, Channel> channelsMap() {
        return channels;
    }
}