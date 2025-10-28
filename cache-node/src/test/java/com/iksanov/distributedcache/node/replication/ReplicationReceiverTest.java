package com.iksanov.distributedcache.node.replication;

import com.iksanov.distributedcache.node.core.CacheStore;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldPrepender;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.net.ServerSocket;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

public class ReplicationReceiverTest {

    private ReplicationReceiver receiver;
    private TestCacheStore store;

    @AfterEach
    public void tearDown() {
        if (receiver != null && receiver.isRunning()) {
            receiver.stop();
        }
    }

    static class TestCacheStore implements CacheStore {
        final Map<String, String> map = new ConcurrentHashMap<>();
        @Override public String get(String key) { return map.get(key); }
        @Override public void put(String key, String value) { map.put(key, value); }
        @Override public void delete(String key) { map.remove(key); }
        @Override public int size() { return map.size(); }
        @Override public void clear() { map.clear(); }
    }

    private static int freePort() throws Exception {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    @Test
    public void lifecycle_and_applySetAndDelete_shouldApplyTasks() throws Exception {
        int port = freePort();
        store = new TestCacheStore();
        receiver = new ReplicationReceiver("127.0.0.1", port, store);
        receiver.start();

        long deadline = System.currentTimeMillis() + 5_000;
        while (!receiver.isRunning() && System.currentTimeMillis() < deadline) Thread.sleep(50);
        assertTrue(receiver.isRunning(), "Receiver must be running");

        NioEventLoopGroup group = new NioEventLoopGroup(1);
        try {
            Bootstrap b = new Bootstrap();
            b.group(group)
                    .channel(NioSocketChannel.class)
                    .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 2000)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override protected void initChannel(SocketChannel ch) {
                            ChannelPipeline p = ch.pipeline();
                            p.addLast(new LengthFieldPrepender(4));
                            p.addLast(new ReplicationMessageCodec());
                        }
                    });

            ChannelFuture cf = b.connect("127.0.0.1", port).sync();
            Channel ch = cf.channel();

            ReplicationTask set = ReplicationTask.ofSet("key-1", "value-1", "origin");
            ch.writeAndFlush(set).sync();

            long wait = System.currentTimeMillis() + 1000;
            while (store.get("key-1") == null && System.currentTimeMillis() < wait) Thread.sleep(20);
            assertEquals("value-1", store.get("key-1"));

            ReplicationTask del = ReplicationTask.ofDelete("key-1", "origin");
            ch.writeAndFlush(del).sync();

            wait = System.currentTimeMillis() + 1000;
            while (store.get("key-1") != null && System.currentTimeMillis() < wait) Thread.sleep(20);
            assertNull(store.get("key-1"));

            ch.close().sync();
        } finally {
            group.shutdownGracefully().await(2, TimeUnit.SECONDS);
            receiver.stop();
        }
    }

    @Test
    public void doubleStartAndStop_shouldNotFail() throws Exception {
        int port = freePort();
        store = new TestCacheStore();
        receiver = new ReplicationReceiver("127.0.0.1", port, store);

        receiver.start();
        long deadline = System.currentTimeMillis() + 2000;
        while (!receiver.isRunning() && System.currentTimeMillis() < deadline) Thread.sleep(50);
        assertTrue(receiver.isRunning());

        receiver.start();
        assertTrue(receiver.isRunning());

        receiver.stop();
        assertFalse(receiver.isRunning());

        receiver.stop();
        assertFalse(receiver.isRunning());
    }

    @Test
    public void customMaxFrameLength_constructor_shouldStartAndStop() throws Exception {
        int port = freePort();
        store = new TestCacheStore();
        int customMax = 64 * 1024;
        receiver = new ReplicationReceiver("127.0.0.1", port, store, customMax);
        receiver.start();

        long deadline = System.currentTimeMillis() + 2000;
        while (!receiver.isRunning() && System.currentTimeMillis() < deadline) Thread.sleep(50);
        assertTrue(receiver.isRunning());

        receiver.stop();
        assertFalse(receiver.isRunning());
    }

    @Test
    public void applyTask_null_shouldBeNoop() throws Exception {
        store = new TestCacheStore();
        receiver = new ReplicationReceiver("127.0.0.1", 0, store);

        Method m = ReplicationReceiver.class.getDeclaredMethod("applyTask", ReplicationTask.class);
        m.setAccessible(true);
        m.invoke(receiver, (ReplicationTask) null);
    }
}
