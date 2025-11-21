package com.iksanov.distributedcache.node.replication;

import com.iksanov.distributedcache.common.cluster.NodeInfo;
import com.iksanov.distributedcache.common.cluster.ReplicaManager;
import com.iksanov.distributedcache.node.metrics.ReplicationMetrics;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
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
    private final ReplicationMetrics metrics;
    private final EventLoopGroup eventLoopGroup;
    private final boolean ownsEventLoopGroup;
    private final Bootstrap bootstrap;
    private final ConcurrentMap<String, Channel> channels = new ConcurrentHashMap<>();
    private volatile boolean shuttingDown = false;

    public ReplicationSender(ReplicaManager replicaManager) {
        this(replicaManager, null, null);
    }

    public ReplicationSender(ReplicaManager replicaManager, EventLoopGroup injectedGroup) {
        this(replicaManager, injectedGroup, null);
    }

    public ReplicationSender(ReplicaManager replicaManager, EventLoopGroup injectedGroup, ReplicationMetrics metrics) {
        this.replicaManager = Objects.requireNonNull(replicaManager, "replicaManager");
        this.metrics = metrics;
        this.ownsEventLoopGroup = (injectedGroup == null);
        this.eventLoopGroup = injectedGroup == null ? new NioEventLoopGroup(2) : injectedGroup;
        this.bootstrap = new Bootstrap()
                .group(this.eventLoopGroup)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 10000)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ChannelPipeline p = ch.pipeline();
                        p.addLast(new LengthFieldPrepender(4));
                        p.addLast(new LengthFieldBasedFrameDecoder(MAX_FRAME_LENGTH, 0, 4, 0, 4));
                        p.addLast(new ReplicationMessageCodec());
                        p.addLast(new SimpleChannelInboundHandler<ReplicationTask>() {
                            @Override
                            protected void channelRead0(ChannelHandlerContext ctx, ReplicationTask task) {
                                if (task.operation() == ReplicationTask.Operation.HEARTBEAT_ACK) {
                                    log.trace("Received HEARTBEAT_ACK from master with sequence={}", task.sequence());
                                } else {
                                    log.debug("Received unexpected message type: {}", task.operation());
                                }
                            }

                            @Override
                            public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                                log.debug("Channel error: {}", cause.getMessage());
                                ctx.close();
                            }
                        });
                    }
                });
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
        String nodeId = replica.nodeId();
        Channel existingChannel = channels.get(nodeId);
        if (existingChannel != null && existingChannel.isActive()) return existingChannel;
        channels.remove(nodeId);
        try {
            ChannelFuture future = bootstrap.connect(replica.host(), replica.replicationPort());
            if (!future.await(1000)) {
                log.warn("[TIMEOUT] Timeout connecting to replica {}", replica);
                return null;
            }
            if (!future.isSuccess()) {
                log.warn("[FAILED] Connection to {} failed: {}", replica, future.cause() != null ? future.cause().getMessage() : "unknown");
                if (metrics != null) metrics.incrementConnectionsFailed();
                return null;
            }
            Channel newChannel = future.channel();
            if (newChannel == null) {
                log.warn("Channel is null for replica {}", replica);
                return null;
            }
            newChannel.closeFuture().addListener(f -> {
                log.info("Channel to {} closed, removing from pool", replica);
                channels.remove(nodeId, newChannel);
                if (metrics != null) metrics.decrementActiveConnections();
            });
            Channel previous = channels.putIfAbsent(nodeId, newChannel);
            if (previous != null && previous != newChannel) {
                if (previous.isActive()) {
                    newChannel.close();
                    return previous;
                } else {
                    channels.replace(nodeId, previous, newChannel);
                }
            }
            log.info("[SUCCESS] Connected to replica: {}", replica);
            if (metrics != null) metrics.incrementConnectionsEstablished();
            return newChannel;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("[FAILED] Interrupted while connecting to {}", replica);
            return null;
        } catch (Exception e) {
            log.error("[FAILED] Failed to connect to {}: {}", replica, e.getMessage());
            return null;
        }
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
                    if (metrics != null) metrics.incrementReplicationsSent();
                } else {
                    log.warn("Write failed for replica {}: {}", replica.nodeId(), future.cause().getMessage());
                    if (metrics != null) metrics.incrementReplicationsFailed();
                    removeChannel(replica.nodeId(), future.channel());
                }
            });
        } catch (Exception e) {
            log.warn("Exception writing to replica {}: {}", replica.nodeId(), e.getMessage());
            removeChannel(replica.nodeId(), ch);
        }
    }

    private void removeChannel(String replicaId, Channel ch) {
        if (channels.remove(replicaId, ch)) closeQuietly(ch);
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

            if (ownsEventLoopGroup) {
                eventLoopGroup.shutdownGracefully(1, 2, TimeUnit.SECONDS).awaitUninterruptibly(3, TimeUnit.SECONDS);
                log.info("EventLoopGroup shutdown complete");
            } else {
                log.debug("Skipping eventLoopGroup shutdown (externally managed)");
            }
            log.info("ReplicationSender shutdown complete");
        } catch (Exception e) {
            log.warn("Error during shutdown: {}", e.getMessage());
        }
    }

    public ConcurrentMap<String, Channel> channelsMap() {
        return channels;
    }

    public void sendHeartbeatToReplicas(NodeInfo master) {
        Set<NodeInfo> replicas = replicaManager.getReplicas(master);
        if (replicas == null || replicas.isEmpty()) {
            log.trace("No replicas configured for master {}, skipping heartbeat", master.nodeId());
            return;
        }

        ReplicationTask heartbeat = ReplicationTask.ofHeartbeat(master.nodeId());
        log.trace("Sending HEARTBEAT to {} replicas", replicas.size());

        for (NodeInfo replica : replicas) {
            if (replica == null || replica.nodeId().equals(master.nodeId())) continue;
            Channel ch = getOrCreateChannel(replica);
            if (ch != null && ch.isActive()) {
                ch.writeAndFlush(heartbeat).addListener(future -> {
                    if (!future.isSuccess()) {
                        log.debug("Failed to send HEARTBEAT to replica {}: {}",
                                replica.nodeId(), future.cause().getMessage());
                    }
                });
            }
        }
    }

    public Channel getChannelToNode(NodeInfo node) {
        return channels.get(node.nodeId());
    }

    public void updateReplicaTargets(NodeInfo newMaster, java.util.List<NodeInfo> replicas) {
        if (newMaster == null) {
            log.warn("updateReplicaTargets called with null newMaster");
            return;
        }

        if (replicas == null || replicas.isEmpty()) {
            log.info("No replicas to register for newly promoted master {}", newMaster.nodeId());
            return;
        }

        log.info("========================================");
        log.info("UPDATING REPLICATION TARGETS");
        log.info("  New Master: {}", newMaster.nodeId());
        log.info("  Replicas: {}", replicas.stream().map(NodeInfo::nodeId).toList());
        log.info("========================================");

        for (NodeInfo replica : replicas) {
            try {
                replicaManager.registerReplica(newMaster, replica);
                log.info("Registered replica {} for master {}", replica.nodeId(), newMaster.nodeId());
            } catch (Exception e) {
                log.error("Failed to register replica {}: {}", replica.nodeId(), e.getMessage(), e);
            }
        }

        Set<NodeInfo> registeredReplicas = replicaManager.getReplicas(newMaster);
        log.info("Replication targets updated: {} replicas registered for master {}", registeredReplicas.size(), newMaster.nodeId());
    }
}