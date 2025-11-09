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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * Production-ready Netty transport implementation for Raft RPCs.
 * Uses TransportMessage as envelope and RaftMessageCodec for serialization.
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
                            p.addLast(new RaftMessageCodec());
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
            if (ch != null && ch.isActive()) {
                ch.writeAndFlush(message);
            } else {
                log.warn("[{}] Channel to {} not active, dropping message {}", localId, target, message.type());
            }
        } catch (Exception e) {
            log.error("[{}] Failed to send message to {} type={}", localId, target, message.type(), e);
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
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ChannelPipeline p = ch.pipeline();
                            p.addLast(new LengthFieldBasedFrameDecoder(10 * 1024 * 1024, 0, 4, 0, 4));
                            p.addLast(new LengthFieldPrepender(4));
                            p.addLast(new RaftMessageCodec());
                            p.addLast(new InboundHandler());
                        }
                    });

            ChannelFuture f = b.connect(host, port).sync();
            log.info("[{}] Connected to peer {}", localId, target);
            return f.channel();
        } catch (Exception e) {
            log.error("[{}] Failed to connect to peer {}", localId, target, e);
            return null;
        }
    }

    private class InboundHandler extends SimpleChannelInboundHandler<TransportMessage> {
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, TransportMessage msg) {
            Consumer<TransportMessage> handler = handlers.get(msg.type());
            if (handler != null) {
                try {
                    handler.accept(msg);
                } catch (Throwable t) {
                    log.error("[{}] Error handling message type={} from={}", localId, msg.type(), msg.from(), t);
                }
            } else {
                log.warn("[{}] No handler registered for message type={} from={}", localId, msg.type(), msg.from());
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            log.error("[{}] Exception in Netty pipeline", localId, cause);
            ctx.close();
        }
    }

    public TransportMessage buildMessage(MessageType type, String to, byte[] payload) {
        long correlationId = correlationCounter.getAndIncrement();
        return new TransportMessage(type, localId, to, correlationId, payload);
    }
}