package com.iksanov.distributedcache.node.replication.integration;

import com.iksanov.distributedcache.node.core.CacheStore;
import com.iksanov.distributedcache.node.replication.ReplicationMessageCodec;
import com.iksanov.distributedcache.node.replication.ReplicationReceiver;
import com.iksanov.distributedcache.node.replication.ReplicationTask;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldPrepender;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.net.ServerSocket;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for {@link ReplicationReceiver}.
 * <p>
 * These tests use REAL network I/O:
 *  - Real Netty server
 *  - Real client connections
 *  - Real message serialization
 *  - Slower execution (10+ seconds)
 * <p>
 * Run with: mvn test -Dgroups="integration"
 */
@Tag("integration")
public class ReplicationReceiverIntegrationTest {

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
        @Override
        public String get(String key) {
            return map.get(key);
        }
        @Override
        public void put(String key, String value) {
            map.put(key, value);
        }
        public void putWithTimestamp(String key, String value, long timestamp) {
            map.put(key, value);
        }
        @Override
        public void delete(String key) {
            map.remove(key);
        }
        @Override
        public int size() {
            return map.size();
        }
        @Override
        public void clear() {
            map.clear();
        }
    }

    private static int freePort() throws Exception {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    @Test
    @DisplayName("Integration: Server should start and accept connections")
    public void shouldStartServerAndAcceptConnections() throws Exception {
        int port = freePort();
        store = new TestCacheStore();
        receiver = new ReplicationReceiver("127.0.0.1", port, store, 1024 * 1024, null, null);
        
        receiver.start();
        awaitRunning(receiver, 2000);

        assertTrue(receiver.isRunning());
    }

    @Test
    @DisplayName("Integration: SET task should replicate over network")
    public void shouldReplicateSetTaskOverNetwork() throws Exception {
        int port = freePort();
        store = new TestCacheStore();
        receiver = new ReplicationReceiver("127.0.0.1", port, store, 1024 * 1024, null, null);
        receiver.start();
        awaitRunning(receiver, 2000);

        NioEventLoopGroup group = new NioEventLoopGroup(1);
        try {
            Channel ch = connectToReceiver(group, port);
            ReplicationTask setTask = ReplicationTask.ofSet("key-1", "value-1", "origin-node", 0L);
            ch.writeAndFlush(setTask).sync();
            awaitValue(store, "key-1", "value-1", 1500);
            assertEquals("value-1", store.get("key-1"));
            ch.close().sync();
        } finally {
            group.shutdownGracefully().await(1, TimeUnit.SECONDS);
        }
    }

    @Test
    @DisplayName("Integration: DELETE task should replicate over network")
    public void shouldReplicateDeleteTaskOverNetwork() throws Exception {
        int port = freePort();
        store = new TestCacheStore();
        store.put("key-to-delete", "initial-value");
        
        receiver = new ReplicationReceiver("127.0.0.1", port, store, 1024 * 1024, null, null);
        receiver.start();
        awaitRunning(receiver, 2000);

        NioEventLoopGroup group = new NioEventLoopGroup(1);
        try {
            Channel ch = connectToReceiver(group, port);
            ReplicationTask deleteTask = ReplicationTask.ofDelete("key-to-delete", "origin-node", 0L);
            ch.writeAndFlush(deleteTask).sync();
            awaitDeletion(store, "key-to-delete", 1500);
            assertNull(store.get("key-to-delete"));
            ch.close().sync();
        } finally {
            group.shutdownGracefully().await(1, TimeUnit.SECONDS);
        }
    }

    @Test
    @DisplayName("Integration: Multiple tasks should be processed in order")
    public void shouldProcessMultipleTasksInOrder() throws Exception {
        int port = freePort();
        store = new TestCacheStore();
        receiver = new ReplicationReceiver("127.0.0.1", port, store, 1024 * 1024, null, null);
        receiver.start();
        awaitRunning(receiver, 2000);

        NioEventLoopGroup group = new NioEventLoopGroup(1);
        try {
            Channel ch = connectToReceiver(group, port);
            ch.writeAndFlush(ReplicationTask.ofSet("k1", "v1", "node-B", 0L)).sync();
            ch.writeAndFlush(ReplicationTask.ofSet("k2", "v2", "node-B", 0L)).sync();
            ch.writeAndFlush(ReplicationTask.ofSet("k3", "v3", "node-B", 0L)).sync();
            awaitValue(store, "k3", "v3", 2000);
            assertEquals("v1", store.get("k1"));
            assertEquals("v2", store.get("k2"));
            assertEquals("v3", store.get("k3"));
            ch.close().sync();
        } finally {
            group.shutdownGracefully().await(1, TimeUnit.SECONDS);
        }
    }

    @Test
    @DisplayName("Integration: Server should handle client disconnect gracefully")
    public void shouldHandleClientDisconnect() throws Exception {
        int port = freePort();
        store = new TestCacheStore();
        receiver = new ReplicationReceiver("127.0.0.1", port, store, 1024 * 1024, null, null);
        receiver.start();
        awaitRunning(receiver, 2000);

        NioEventLoopGroup group = new NioEventLoopGroup(1);
        try {
            Channel ch = connectToReceiver(group, port);
            ch.writeAndFlush(ReplicationTask.ofSet("key", "value", "node-B", 0L)).sync();
            awaitValue(store, "key", "value", 1500);
            ch.close().sync();
            Thread.sleep(100);
            assertTrue(receiver.isRunning(), "Server should still be running after client disconnect");
        } finally {
            group.shutdownGracefully().await(1, TimeUnit.SECONDS);
        }
    }

    @Test
    @DisplayName("Integration: Stop should close server and release port")
    public void shouldStopServerAndReleasePort() throws Exception {
        int port = freePort();
        store = new TestCacheStore();
        receiver = new ReplicationReceiver("127.0.0.1", port, store, 1024 * 1024, null, null);
        receiver.start();
        awaitRunning(receiver, 2000);
        receiver.stop();
        assertFalse(receiver.isRunning());
        ReplicationReceiver receiver2 = new ReplicationReceiver("127.0.0.1", port, store, 1024 * 1024, null, null);
        receiver2.start();
        awaitRunning(receiver2, 2000);
        assertTrue(receiver2.isRunning());
        receiver2.stop();
    }

    private Channel connectToReceiver(NioEventLoopGroup group, int port) throws InterruptedException {
        Bootstrap b = new Bootstrap();
        b.group(group)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 2000)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ChannelPipeline p = ch.pipeline();
                        p.addLast(new LengthFieldPrepender(4));
                        p.addLast(new ReplicationMessageCodec());
                    }
                });

        ChannelFuture cf = b.connect("127.0.0.1", port).sync();
        return cf.channel();
    }

    private void awaitRunning(ReplicationReceiver receiver, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (!receiver.isRunning() && System.currentTimeMillis() < deadline) {
            Thread.sleep(50);
        }
        if (!receiver.isRunning()) {
            throw new AssertionError("Receiver did not start within timeout");
        }
    }

    private void awaitValue(TestCacheStore store, String key, String expectedValue, long timeoutMs) 
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (expectedValue.equals(store.get(key))) return;
            Thread.sleep(20);
        }
        fail("Timeout waiting for value: key=" + key + ", expected=" + expectedValue + 
             ", actual=" + store.get(key));
    }

    private void awaitDeletion(TestCacheStore store, String key, long timeoutMs) 
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (store.get(key) == null) {
                return;
            }
            Thread.sleep(20);
        }
        fail("Timeout waiting for deletion: key=" + key);
    }
}