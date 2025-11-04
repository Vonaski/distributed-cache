package com.iksanov.distributedcache.node.consensus.transport;

import com.iksanov.distributedcache.node.consensus.RaftNode;
import com.iksanov.distributedcache.node.consensus.model.*;
import com.iksanov.distributedcache.node.metrics.RaftMetrics;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.*;

/**
 * Production-ready Netty transport using RaftMessageCodec.
 * <p>
 * Usage:
 *   NettyRaftTransport transport = new NettyRaftTransport(serverPort, raftNode);
 *   transport.startServer();
 *   // use transport.requestVote(target, req, timeout) etc.
 *   transport.shutdown();
 */
public class NettyRaftTransport {
    private static final Logger log = LoggerFactory.getLogger(NettyRaftTransport.class);
    private final RaftMessageCodec codec = new RaftMessageCodec();
    private final RaftNode raftNode;
    private final RaftMetrics metrics;
    private final int serverPort;
    private final NioEventLoopGroup bossGroup = new NioEventLoopGroup(1);
    private final NioEventLoopGroup workerGroup = new NioEventLoopGroup(1);
    private final NioEventLoopGroup clientGroup = new NioEventLoopGroup(1);
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
    private final ConcurrentMap<String, CompletableFuture<Object>> pending = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Channel> channels = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, ScheduledFuture<?>> reconnects = new ConcurrentHashMap<>();
    private final Duration defaultTimeout = Duration.ofMillis(1000);
    private final Duration reconnectDelay = Duration.ofSeconds(1);
    private final int maxFrame = 64 * 1024;
    private Channel serverChannel;

    public NettyRaftTransport(int serverPort, RaftNode raftNode, RaftMetrics metrics) {
        this.serverPort = serverPort;
        this.raftNode = Objects.requireNonNull(raftNode);
        this.metrics = Objects.requireNonNull(metrics);
    }

    public void startServer() throws InterruptedException {
        ServerBootstrap sb = new ServerBootstrap();
        sb.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ChannelPipeline p = ch.pipeline();
                        p.addLast(new LengthFieldBasedFrameDecoder(maxFrame, 0, 4, 0, 4));
                        p.addLast(codec);
                        p.addLast(new ServerHandler());
                    }
                });
        ChannelFuture f = sb.bind(serverPort).sync();
        serverChannel = f.channel();
        log.info("🚀 [Raft] Transport server started on port {}", serverPort);
    }

    public void shutdown() {
        log.info("🧩 [Raft] Shutting down transport...");
        try {
            if (serverChannel != null) serverChannel.close().syncUninterruptibly();
        } catch (Exception ignored) {}
        bossGroup.shutdownGracefully();
        workerGroup.shutdownGracefully();
        clientGroup.shutdownGracefully();
        scheduler.shutdownNow();
        pending.forEach((id, cf) -> cf.completeExceptionally(new RuntimeException("Transport shutdown")));
        pending.clear();
        channels.forEach((k, ch) -> {
            try { if (ch != null && ch.isOpen()) ch.close().syncUninterruptibly(); } catch (Exception ignored) {}
        });
        channels.clear();
        log.info("✅ [Raft] Transport shutdown complete");
    }

    public CompletableFuture<VoteResponse> requestVote(String target, VoteRequest req, Duration timeout) {
        metrics.incrementHeartbeatsSent();
        Duration t = (timeout == null) ? defaultTimeout : timeout;
        String id = UUID.randomUUID().toString();
        CompletableFuture<VoteResponse> cf = new CompletableFuture<>();
        pending.put(id, (CompletableFuture<Object>) (CompletableFuture<?>) cf);
        Channel ch = getOrConnect(target);
        if (ch == null || !ch.isActive()) {
            scheduleReconnect(target);
            pending.remove(id);
            cf.completeExceptionally(new RuntimeException("No connection to target " + target));
            log.warn("⚠️ [Raft] Vote request failed: no active channel to {}", target);
            return cf;
        }

        RaftMessageCodec.CorrelatedVoteRequest out = new RaftMessageCodec.CorrelatedVoteRequest(id, req);
        ch.writeAndFlush(out).addListener((ChannelFutureListener) f -> {
            if (!f.isSuccess()) {
                log.warn("⚠️ [Raft] Failed to send VoteRequest to {}: {}", target, f.cause().getMessage());
                CompletableFuture<Object> p = pending.remove(id);
                if (p != null) p.completeExceptionally(f.cause());
            } else {
                log.debug("📤 [Raft] VoteRequest sent to {}", target);
            }
        });

        scheduler.schedule(() -> {
            CompletableFuture<Object> p = pending.remove(id);
            if (p != null && !p.isDone()) {
                log.debug("⌛ [Raft] VoteRequest to {} timed out", target);
                p.completeExceptionally(new TimeoutException("Timeout waiting for VoteResponse"));
            }
        }, t.toMillis(), TimeUnit.MILLISECONDS);
        return cf;
    }

    public CompletableFuture<HeartbeatResponse> sendHeartbeat(String target, HeartbeatRequest req, Duration timeout) {
        metrics.incrementHeartbeatsSent();
        Duration t = (timeout == null) ? defaultTimeout : timeout;
        String id = UUID.randomUUID().toString();
        CompletableFuture<HeartbeatResponse> cf = new CompletableFuture<>();
        pending.put(id, (CompletableFuture<Object>) (CompletableFuture<?>) cf);

        Channel ch = getOrConnect(target);
        if (ch == null || !ch.isActive()) {
            scheduleReconnect(target);
            pending.remove(id);
            cf.completeExceptionally(new RuntimeException("No connection to target " + target));
            log.debug("⚠️ [Raft] Heartbeat failed: no connection to {}", target);
            return cf;
        }

        RaftMessageCodec.CorrelatedHeartbeatRequest out = new RaftMessageCodec.CorrelatedHeartbeatRequest(id, req);
        ch.writeAndFlush(out).addListener((ChannelFutureListener) f -> {
            if (!f.isSuccess()) {
                log.warn("⚠️ [Raft] Failed to send Heartbeat to {}: {}", target, f.cause().getMessage());
                CompletableFuture<Object> p = pending.remove(id);
                if (p != null) p.completeExceptionally(f.cause());
            } else {
                log.trace("📤 [Raft] Heartbeat sent to {}", target);
            }
        });

        scheduler.schedule(() -> {
            CompletableFuture<Object> p = pending.remove(id);
            if (p != null && !p.isDone()) {
                log.trace("⌛ [Raft] Heartbeat to {} timed out", target);
                p.completeExceptionally(new TimeoutException("Timeout waiting for HeartbeatResponse"));
            }
        }, t.toMillis(), TimeUnit.MILLISECONDS);
        return cf;
    }

    private Channel getOrConnect(String target) {
        Channel ch = channels.get(target);
        if (ch != null && ch.isActive()) return ch;

        synchronized (target.intern()) {
            ch = channels.get(target);
            if (ch != null && ch.isActive()) return ch;

            String[] parts = target.split(":");
            if (parts.length != 2) return null;
            String host = parts[0];
            int port = Integer.parseInt(parts[1]);

            Bootstrap b = new Bootstrap();
            b.group(clientGroup)
                    .channel(NioSocketChannel.class)
                    .option(ChannelOption.SO_KEEPALIVE, true)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ChannelPipeline p = ch.pipeline();
                            p.addLast(new LengthFieldPrepender(4));
                            p.addLast(new LengthFieldBasedFrameDecoder(maxFrame, 0, 4, 0, 4));
                            p.addLast(codec);
                            p.addLast(new ClientHandler());
                        }
                    });
            try {
                ChannelFuture f = b.connect(new InetSocketAddress(host, port)).sync();
                Channel newCh = f.channel();
                channels.put(target, newCh);
                newCh.closeFuture().addListener((future) -> {
                    channels.remove(target, newCh);
                    scheduleReconnect(target);
                });
                log.info("🔌 [Raft] Connected to peer {}", target);
                return newCh;
            } catch (Exception e) {
                log.warn("⚠️ [Raft] Failed connect to {}: {}, scheduling reconnect", target, e.getMessage());
                scheduleReconnect(target);
                return null;
            }
        }
    }

    private void scheduleReconnect(String target) {
        reconnects.compute(target, (k, existing) -> {
            if (existing != null && !existing.isDone()) return existing;
            return scheduler.schedule(() -> {
                channels.remove(target);
                getOrConnect(target);
            }, reconnectDelay.toMillis(), TimeUnit.MILLISECONDS);
        });
    }

    private class ServerHandler extends SimpleChannelInboundHandler<RaftMessageCodec.MessageEnvelope> {
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, RaftMessageCodec.MessageEnvelope envelope) {
            Object payload = envelope.msg();
            String id = envelope.id();
            try {
                if (payload instanceof VoteRequest vr) {
                    log.debug("📥 [Raft] Received VoteRequest from {}", ctx.channel().remoteAddress());
                    VoteResponse resp = raftNode.handleVoteRequest(vr);
                    ctx.writeAndFlush(new RaftMessageCodec.CorrelatedVoteResponse(id, resp));
                } else if (payload instanceof HeartbeatRequest hr) {
                    log.trace("📥 [Raft] Received Heartbeat from {}", ctx.channel().remoteAddress());
                    HeartbeatResponse resp = raftNode.handleHeartbeat(hr);
                    ctx.writeAndFlush(new RaftMessageCodec.CorrelatedHeartbeatResponse(id, resp));
                    metrics.incrementHeartbeatsReceived();
                } else {
                    log.warn("⚠️ [Raft] Unknown message type: {}", payload.getClass());
                }
            } catch (Exception e) {
                log.error("💥 [Raft] Error processing inbound message: {}", e.getMessage(), e);
            }
        }
        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            log.error("💥 [Raft] Server handler error: {}", cause.getMessage());
            ctx.close();
        }
    }

    private class ClientHandler extends SimpleChannelInboundHandler<RaftMessageCodec.MessageEnvelope> {
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, RaftMessageCodec.MessageEnvelope envelope) {
            String id = envelope.id();
            Object msg = envelope.msg();
            CompletableFuture<Object> cf = pending.remove(id);
            if (cf == null) {
                log.debug("⚠️ [Raft] No pending future for id {}", id);
                return;
            }
            if (msg instanceof VoteResponse vr) {
                ((CompletableFuture<VoteResponse>) (CompletableFuture<?>) cf).complete(vr);
                if (vr.voteGranted) metrics.incrementVoteGranted();
                else metrics.incrementVoteDenied();
            } else if (msg instanceof HeartbeatResponse hr) {
                ((CompletableFuture<HeartbeatResponse>) (CompletableFuture<?>) cf).complete(hr);
                metrics.incrementHeartbeatsReceived();
            } else {
                cf.completeExceptionally(new RuntimeException("Unexpected response type: " + msg.getClass()));
                log.warn("⚠️ [Raft] Unexpected message type: {}", msg.getClass().getSimpleName());
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            log.error("💥 [Raft] Client handler error: {}", cause.getMessage());
            ctx.close();
        }
    }
}
