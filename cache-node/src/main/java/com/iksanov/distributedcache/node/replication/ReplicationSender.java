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
 * ReplicationSender — минимально-инвазивный, устойчивый sender:
 *  - reuses Channel per replica
 *  - safe shutdown that ALWAYS clears channels map
 *  - defensive close handling (works with mocks)
 */
public class ReplicationSender {

    private static final Logger log = LoggerFactory.getLogger(ReplicationSender.class);

    private final ReplicaManager replicaManager;
    private final EventLoopGroup eventLoopGroup;
    private final Bootstrap bootstrap;
    private final ConcurrentMap<String, Channel> channels = new ConcurrentHashMap<>();

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
                .option(ChannelOption.SO_KEEPALIVE, true);
    }

    public void replicate(NodeInfo master, ReplicationTask task) {
        log.info("Replicating task {} from {} → replicas: {}", task.key(), master.nodeId(), replicaManager.getReplicas(master));
        Set<NodeInfo> replicas = replicaManager.getReplicas(master);
        if (replicas == null || replicas.isEmpty()) {
            log.warn("No replicas found for master {}", master.nodeId());
            return;
        }

        for (NodeInfo replica : replicas) {
            if (replica == null) continue;
            if (master != null && replica.nodeId().equals(master.nodeId())) continue;
            send(replica, task);
        }
    }

    private void send(NodeInfo replica, ReplicationTask task) {
        Channel ch = channels.get(replica.nodeId());
        if (ch != null && ch.isActive()) {
            safeWrite(replica, ch, task);
            return;
        }

        try {
            Bootstrap child = bootstrap.clone();
            child.handler(new ChannelInitializer<>() {
                @Override
                protected void initChannel(Channel ch) {
                    ChannelPipeline p = ch.pipeline();
                    p.addLast(new LengthFieldPrepender(4));
                    p.addLast(new LengthFieldBasedFrameDecoder(1024 * 1024, 0, 4, 0, 4));
                    p.addLast(new ReplicationMessageCodec());
                }
            });

            ChannelFuture f = child.connect(replica.host(), replica.replicationPort()).syncUninterruptibly();
            if (f.isSuccess()) {
                log.info("Connected to replica {} at {}:{}", replica.nodeId(), replica.host(), replica.replicationPort());
                Channel newCh = f.channel();
                channels.put(replica.nodeId(), newCh);
                safeWrite(replica, newCh, task);
            } else {
                log.warn("Connect to replica {} failed: {}", replica.nodeId(),
                        f.cause() != null ? f.cause().toString() : "unknown");
            }
        } catch (Exception e) {
            log.warn("Connect to replica {} failed: {}", replica.nodeId(), e.toString());
        }
    }

    private void safeWrite(NodeInfo replica, Channel ch, ReplicationTask task) {
        try {
            ChannelFuture cf = ch.writeAndFlush(task);
            if (cf == null) {
                log.warn("writeAndFlush returned null future for replica {}, skipping", replica.nodeId());
                channels.remove(replica.nodeId());
                return;
            }

            cf.addListener((ChannelFutureListener) future -> {
                if (!future.isSuccess()) {
                    log.warn("Write to replica {} failed: {}", replica.nodeId(),
                            future.cause() != null ? future.cause().toString() : "unknown");
                    channels.remove(replica.nodeId());
                    try {
                        Channel c = future.channel();
                        if (c != null && c.isOpen()) c.close();
                    } catch (Exception ignored) {}
                } else {
                    log.debug("Write to replica {} succeeded", replica.nodeId());
                }
            });
        } catch (Exception e) {
            log.warn("Exception while sending to replica {}: {}", replica.nodeId(), e.toString());
            channels.remove(replica.nodeId());
        }
    }

    public void shutdown() {
        log.info("Shutting down ReplicationSender...");
        try {
            for (Channel c : channels.values()) {
                if (c == null) continue;
                try {
                    boolean open;
                    try {
                        open = c.isOpen();
                    } catch (Throwable ignored) {
                        open = false;
                    }
                    if (open) {
                        ChannelFuture f = null;
                        try {
                            f = c.close();
                        } catch (Throwable ex) {
                            log.debug("channel.close() threw: {}", ex.toString());
                        }
                        if (f != null) {
                            try {
                                f.syncUninterruptibly();
                            } catch (Throwable ignored) {}
                        }
                    }
                } catch (Throwable ignore) {
                }
            }

            try {
                eventLoopGroup.shutdownGracefully().awaitUninterruptibly(2, TimeUnit.SECONDS);
            } catch (Throwable ignored) {
            }

            log.info("ReplicationSender shut down successfully.");
        } finally {
            channels.clear();
        }
    }

    ConcurrentMap<String, Channel> channelsMap() {
        return channels;
    }
}
