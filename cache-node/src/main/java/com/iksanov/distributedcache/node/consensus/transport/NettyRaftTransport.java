package com.iksanov.distributedcache.node.consensus.transport;

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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * Production-ready Netty transport implementation for Raft RPCs.
 * Uses TransportMessageCodec for unified envelope and RPC serialization.
 * <p>
 * Works in both directions (server accepts, client connects) and delivers messages asynchronously.
 */
public class NettyRaftTransport implements RaftTransport {
    private static final Logger log = LoggerFactory.getLogger(NettyRaftTransport.class);

    private final String localId;
    private final String host;
    private final int port;
    private final Map<MessageType, Consumer<TransportMessage>> handlers = new ConcurrentHashMap<>();
    private final Map<String, Channel> channels = new ConcurrentHashMap<>();
    private final Map<String, String> nodeIdToAddress = new ConcurrentHashMap<>(); // nodeId -> "host:port"
    private final EventLoopGroup bossGroup = new NioEventLoopGroup(1);
    private final EventLoopGroup workerGroup = new NioEventLoopGroup(1);
    private final EventLoopGroup clientGroup = new NioEventLoopGroup();
    private final AtomicLong correlationCounter = new AtomicLong(1);
    private Channel serverChannel;
    public NettyRaftTransport(String localId, String host, int port) {
        this.localId = localId;
        this.host = host;
        this.port = port;
    }

    /**
     * Register mapping from nodeId to network address for peers.
     * This is needed because messages contain nodeId in 'from' field,
     * but we need network address to send replies.
     */
    public void registerPeerMapping(String nodeId, String address) {
        nodeIdToAddress.put(nodeId, address);
        log.debug("[{}] Registered peer mapping: {} -> {}", localId, nodeId, address);
    }

    @Override
    public void connectToPeers(List<String> peers) throws InterruptedException {
        log.info("[{}] Pre-connecting to {} peers...", localId, peers.size());

        java.util.List<java.util.concurrent.CompletableFuture<Void>> futures = new java.util.ArrayList<>();

        for (String peer : peers) {
            java.util.concurrent.CompletableFuture<Void> future = new java.util.concurrent.CompletableFuture<>();
            futures.add(future);

            try {
                String[] parts = peer.split(":");
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
                                p.addLast(new LengthFieldBasedFrameDecoder(10 * 1024 * 1024, 0, 4, 0, 4));
                                p.addLast(new LengthFieldPrepender(4));
                                p.addLast(new TransportMessageCodec());
                                p.addLast(new InboundHandler());
                            }
                        });

                ChannelFuture cf = b.connect(host, port);
                cf.addListener((ChannelFutureListener) channelFuture -> {
                    if (channelFuture.isSuccess()) {
                        Channel channel = channelFuture.channel();
                        channels.put(peer, channel);

                        // Wait for channel to become active (ready to send)
                        if (channel.isActive()) {
                            log.info("[{}] Pre-connected to peer {} (already active)", localId, peer);
                            future.complete(null);
                        } else {
                            // Wait for channelActive event
                            channel.pipeline().addLast(new io.netty.channel.ChannelInboundHandlerAdapter() {
                                @Override
                                public void channelActive(ChannelHandlerContext ctx) throws Exception {
                                    log.info("[{}] Pre-connected to peer {} (now active)", localId, peer);
                                    future.complete(null);
                                    ctx.pipeline().remove(this);
                                    super.channelActive(ctx);
                                }
                            });
                        }
                    } else {
                        log.warn("[{}] Failed to pre-connect to peer {}: {}", localId, peer, channelFuture.cause().getMessage());
                        future.completeExceptionally(channelFuture.cause());
                    }
                });
            } catch (Exception e) {
                log.error("[{}] Error initiating connection to peer {}", localId, peer, e);
                future.completeExceptionally(e);
            }
        }

        // Wait for all connections with timeout
        try {
            java.util.concurrent.CompletableFuture.allOf(futures.toArray(new java.util.concurrent.CompletableFuture[0]))
                    .get(5, java.util.concurrent.TimeUnit.SECONDS);
            log.info("[{}] All peer connections established successfully", localId);
        } catch (java.util.concurrent.TimeoutException e) {
            log.warn("[{}] Timeout waiting for some peer connections", localId);
        } catch (java.util.concurrent.ExecutionException e) {
            log.warn("[{}] Some peer connections failed: {}", localId, e.getMessage());
        }
    }

    @Override
    public void start() {
        try {
            ServerBootstrap sb = new ServerBootstrap();
            sb.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ChannelPipeline p = ch.pipeline();
                            p.addLast(new LengthFieldBasedFrameDecoder(10 * 1024 * 1024, 0, 4, 0, 4));
                            p.addLast(new LengthFieldPrepender(4));
                            p.addLast(new TransportMessageCodec());
                            p.addLast(new InboundHandler());
                        }
                    })
                    .option(ChannelOption.SO_BACKLOG, 128)
                    .childOption(ChannelOption.SO_KEEPALIVE, true);

            ChannelFuture f = sb.bind(new InetSocketAddress(host, port)).sync();
            serverChannel = f.channel();
            log.info("NettyRaftTransport [{}] started on {}:{}", localId, host, port);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Failed to start NettyRaftTransport", e);
        }
    }

    @Override
    public void shutdown() {
        try {
            if (serverChannel != null) {
                serverChannel.close().sync();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
            clientGroup.shutdownGracefully();
            log.info("NettyRaftTransport [{}] stopped", localId);
        }
    }

    @Override
    public void send(TransportMessage message) {
        String target = message.to();
        try {
            Channel ch = channels.computeIfAbsent(target, this::createChannel);
            if (ch == null) {
                log.warn("[{}] Failed to create channel to {}, dropping message type={}", localId, target, message.type());
                return;
            }

            if (ch.isActive()) {
                ch.writeAndFlush(message);
            } else if (ch.isOpen() && !ch.isActive()) {
                log.debug("[{}] Channel to {} is still connecting, dropping message type={}", localId, target, message.type());
            } else {
                log.debug("[{}] Channel to {} is closed, will reconnect on next attempt", localId, target);
                channels.remove(target);
            }
        } catch (Exception e) {
            log.error("[{}] Failed to send message to {} type={}", localId, target, message.type(), e);
            channels.remove(target);
        }
    }

    @Override
    public void registerHandler(MessageType type, Consumer<TransportMessage> handler) {
        handlers.put(type, handler);
    }

    private Channel createChannel(String target) {
        try {
            String[] parts = target.split(":");
            String host = parts[0];
            int port = Integer.parseInt(parts[1]);

            Bootstrap b = new Bootstrap();
            b.group(clientGroup)
                    .channel(NioSocketChannel.class)
                    .option(ChannelOption.SO_KEEPALIVE, true)
                    .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 3000)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ChannelPipeline p = ch.pipeline();
                            p.addLast(new LengthFieldBasedFrameDecoder(10 * 1024 * 1024, 0, 4, 0, 4));
                            p.addLast(new LengthFieldPrepender(4));
                            p.addLast(new TransportMessageCodec());
                            p.addLast(new InboundHandler());
                        }
                    });

            ChannelFuture f = b.connect(host, port).sync();

            if (f.isSuccess()) {
                Channel channel = f.channel();
                log.debug("[{}] Successfully connected to peer {}", localId, target);
                return channel;
            } else {
                log.warn("[{}] Failed to connect to peer {}: {}", localId, target, f.cause().getMessage());
                return null;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("[{}] Interrupted while connecting to peer {}", localId, target);
            return null;
        } catch (Exception e) {
            log.error("[{}] Failed to create connection to peer {}", localId, target, e);
            return null;
        }
    }
    private class InboundHandler extends SimpleChannelInboundHandler<TransportMessage> {

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, TransportMessage msg) {
            String senderNodeId = msg.from();
            String senderAddress = nodeIdToAddress.get(senderNodeId);

            if (senderAddress == null) {
                InetSocketAddress remoteAddress = (InetSocketAddress) ctx.channel().remoteAddress();
                log.debug("[{}] Received {} from nodeId={}, remoteAddr={}:{}, no mapping found",
                    localId, msg.type(), senderNodeId, remoteAddress.getHostString(), remoteAddress.getPort());

                for (Map.Entry<String, Channel> entry : channels.entrySet()) {
                    Channel channel = entry.getValue();
                    if (channel.isActive()) {
                        InetSocketAddress channelRemote = (InetSocketAddress) channel.remoteAddress();
                        if (channelRemote.getHostString().equals(remoteAddress.getHostString()) &&
                            channelRemote.getPort() == remoteAddress.getPort()) {
                            senderAddress = entry.getKey();
                            log.debug("[{}] Mapped nodeId={} to address={} via channel matching",
                                localId, senderNodeId, senderAddress);
                            break;
                        }
                    }
                }
                if (senderAddress == null) {
                    senderAddress = remoteAddress.getHostString() + ":" + remoteAddress.getPort();
                    log.warn("[{}] Could not map nodeId={}, using remoteAddr={}",
                        localId, senderNodeId, senderAddress);
                }
            }
            TransportMessage correctedMsg = new TransportMessage(
                msg.type(),
                senderAddress,
                msg.to(),
                msg.correlationId(),
                msg.payload()
            );

            Consumer<TransportMessage> handler = handlers.get(correctedMsg.type());
            if (handler != null) {
                try {
                    handler.accept(correctedMsg);
                } catch (Throwable t) {
                    log.error("[{}] Error handling message type={} from={}", localId, correctedMsg.type(), correctedMsg.from(), t);
                }
            } else {
                log.warn("[{}] No handler registered for message type={} from={}", localId, correctedMsg.type(), correctedMsg.from());
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            log.error("[{}] Exception in Netty pipeline", localId, cause);
            ctx.close();
        }
    }

    public TransportMessage buildMessage(MessageType type, String to, Object payload) {
        long correlationId = correlationCounter.getAndIncrement();
        return new TransportMessage(type, localId, to, correlationId, payload);
    }
}