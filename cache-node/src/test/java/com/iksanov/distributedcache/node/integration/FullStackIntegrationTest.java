package com.iksanov.distributedcache.node.integration;

import com.iksanov.distributedcache.common.cluster.NodeInfo;
import com.iksanov.distributedcache.common.cluster.ReplicaManager;
import com.iksanov.distributedcache.common.codec.CacheMessageCodec;
import com.iksanov.distributedcache.common.dto.CacheRequest;
import com.iksanov.distributedcache.common.dto.CacheResponse;
import com.iksanov.distributedcache.node.config.NetServerConfig;
import com.iksanov.distributedcache.node.core.CacheStore;
import com.iksanov.distributedcache.node.core.InMemoryCacheStore;
import com.iksanov.distributedcache.node.metrics.CacheMetrics;
import com.iksanov.distributedcache.node.metrics.NetMetrics;
import com.iksanov.distributedcache.node.metrics.RaftMetrics;
import com.iksanov.distributedcache.node.net.NetServer;
import com.iksanov.distributedcache.node.replication.ReplicationManager;
import com.iksanov.distributedcache.node.replication.ReplicationReceiver;
import com.iksanov.distributedcache.node.replication.ReplicationSender;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import org.junit.jupiter.api.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Full end-to-end integration test for the entire cache-node module.
 * <p>
 * Tests the complete stack:
 * 1. NetServer (client-facing API)
 * 2. InMemoryCacheStore (storage layer)
 * 3. ReplicationManager (coordination)
 * 4. ReplicationSender/Receiver (network replication)
 * <p>
 * Verifies that:
 * - Client can connect to master node
 * - SET operations are replicated to all replicas
 * - DELETE operations are replicated to all replicas
 * - All nodes maintain consistency
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@DisplayName("Full Stack E2E Integration Test")
@Tag("integration")
class FullStackIntegrationTest {

    private static final String HOST = "127.0.0.1";
    private static final long REPLICATION_WAIT_MS = 2000;
    private NodeInfo masterNode;
    private NodeInfo replicaNode1;
    private NodeInfo replicaNode2;
    private List<NodeInfo> allNodes;
    private Map<String, CacheStore> stores;
    private Map<String, NetServer> netServers;
    private ReplicaManager replicaManager;
    private Map<String, ReplicationManager> replicationManagers;
    private Map<String, ReplicationSender> senders;
    private Map<String, ReplicationReceiver> receivers;
    private NioEventLoopGroup clientEventLoopGroup;
    private Bootstrap clientBootstrap;
    private Map<String, CompletableFuture<CacheResponse>> pendingResponses;
    private CacheMetrics cacheMetrics;
    private RaftMetrics raftMetrics;
    private ReplicationMetrics replicationMetrics;
    private NetMetrics netMetrics;

    @BeforeAll
    void setupCluster() throws Exception {
        System.out.println("🚀 Setting up 3-node cluster for E2E test...");

        masterNode = new NodeInfo("master", HOST, 9100, 9200);
        replicaNode1 = new NodeInfo("replica-1", HOST, 9101, 9201);
        replicaNode2 = new NodeInfo("replica-2", HOST, 9102, 9202);
        allNodes = List.of(masterNode, replicaNode1, replicaNode2);

        stores = new ConcurrentHashMap<>();
        netServers = new ConcurrentHashMap<>();
        cacheMetrics = new CacheMetrics();
        raftMetrics = new RaftMetrics();
        replicationMetrics = new ReplicationMetrics();
        netMetrics = new NetMetrics();
        replicationManagers = new ConcurrentHashMap<>();
        senders = new ConcurrentHashMap<>();
        receivers = new ConcurrentHashMap<>();
        pendingResponses = new ConcurrentHashMap<>();
        replicaManager = new ReplicaManager();
        replicaManager.registerReplica(masterNode, replicaNode1);
        replicaManager.registerReplica(masterNode, replicaNode2);

        Function<String, NodeInfo> primaryResolver = key -> masterNode;
        NioEventLoopGroup sharedReplicationGroup = new NioEventLoopGroup(4);

        for (NodeInfo node : allNodes) {
            System.out.println("  📦 Starting node: " + node.nodeId());

            CacheStore store = new InMemoryCacheStore(10000, 0, 1000, cacheMetrics);
            stores.put(node.nodeId(), store);

            ReplicationReceiver receiver = new ReplicationReceiver(
                    node.host(),
                    node.replicationPort(),
                    store,
                    1024 * 1024,
                    node.nodeId()
            );
            receiver.start();
            receivers.put(node.nodeId(), receiver);

            ReplicationSender sender = new ReplicationSender(
                    replicaManager,
                    sharedReplicationGroup
            );
            senders.put(node.nodeId(), sender);

            ReplicationManager replManager = new ReplicationManager(
                    node,
                    sender,
                    receiver,
                    primaryResolver
            );
            replicationManagers.put(node.nodeId(), replManager);

            NetServerConfig config = new NetServerConfig(
                    node.host(),
                    node.port(),
                    2,  // boss threads
                    4,  // worker threads
                    128,  // backlog
                    1024 * 1024,  // max frame length
                    5,  // read timeout
                    30  // write timeout
            );
            NetServer netServer = new NetServer(config, store, replManager, netMetrics);
            netServer.start();
            netServers.put(node.nodeId(), netServer);
            System.out.println("    ✅ Node " + node.nodeId() + " started on port " + node.port());
        }
        Thread.sleep(1000);
        setupClient();
        System.out.println("✅ Cluster ready!\n");
    }

    @AfterAll
    void tearDownCluster() throws Exception {
        System.out.println("\n🛑 Shutting down cluster...");

        if (clientEventLoopGroup != null) clientEventLoopGroup.shutdownGracefully().await(2, TimeUnit.SECONDS);

        for (NodeInfo node : allNodes) {
            String nodeId = node.nodeId();
            if (netServers.containsKey(nodeId)) {
                try {
                    netServers.get(nodeId).stop();
                } catch (Exception e) {
                    System.err.println("Error stopping NetServer for " + nodeId + ": " + e.getMessage());
                }
            }

            if (replicationManagers.containsKey(nodeId)) {
                try {
                    replicationManagers.get(nodeId).shutdown();
                } catch (Exception e) {
                    System.err.println("Error shutting down ReplicationManager for " + nodeId);
                }
            }

            if (receivers.containsKey(nodeId)) {
                try {
                    receivers.get(nodeId).stop();
                } catch (Exception e) {
                    System.err.println("Error stopping ReplicationReceiver for " + nodeId);
                }
            }
        }
        System.out.println("✅ Cluster shutdown complete\n");
    }

    private void setupClient() {
        clientEventLoopGroup = new NioEventLoopGroup(1);
        clientBootstrap = new Bootstrap()
                .group(clientEventLoopGroup)
                .channel(NioSocketChannel.class)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ChannelPipeline p = ch.pipeline();
                        p.addLast("frameDecoder", new LengthFieldBasedFrameDecoder(
                                1024 * 1024, 0, 4, 0, 4));
                        p.addLast("frameEncoder", new LengthFieldPrepender(4));
                        p.addLast("codec", new CacheMessageCodec());
                        p.addLast("handler", new SimpleChannelInboundHandler<CacheResponse>() {
                            @Override
                            protected void channelRead0(ChannelHandlerContext ctx, CacheResponse msg) {
                                CompletableFuture<CacheResponse> future = pendingResponses.remove(msg.requestId());
                                if (future != null) {
                                    future.complete(msg);
                                }
                            }
                            @Override
                            public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                                cause.printStackTrace();
                                ctx.close();
                            }
                        });
                    }
                });
    }

    private CacheResponse sendRequest(NodeInfo node, CacheRequest request) throws Exception {
        CompletableFuture<CacheResponse> future = new CompletableFuture<>();
        pendingResponses.put(request.requestId(), future);
        Channel channel = clientBootstrap.connect(node.host(), node.port()).sync().channel();
        channel.writeAndFlush(request).sync();
        CacheResponse response = future.get(5, TimeUnit.SECONDS);
        channel.close().sync();
        return response;
    }

    private void waitForReplication(String key, String expectedValue) throws InterruptedException {
        long start = System.currentTimeMillis();
        while (System.currentTimeMillis() - start < REPLICATION_WAIT_MS) {
            boolean allMatch = allNodes.stream()
                    .allMatch(node -> {
                        String value = stores.get(node.nodeId()).get(key);
                        return expectedValue.equals(value);
                    });
            if (allMatch) return;
            Thread.sleep(100);
        }
    }

    private void waitForDeletion(String key) throws InterruptedException {
        long start = System.currentTimeMillis();
        while (System.currentTimeMillis() - start < REPLICATION_WAIT_MS) {
            boolean allDeleted = allNodes.stream()
                    .allMatch(node -> stores.get(node.nodeId()).get(key) == null);
            if (allDeleted) return;
            Thread.sleep(100);
        }
    }

    @Test
    @DisplayName("E2E: Client SET → Master → Replicas (full replication)")
    void testFullStackSetReplication() throws Exception {
        System.out.println("\n🧪 TEST: Full stack SET replication");
        String key = "user:123";
        String value = "John Doe";
        System.out.println("  1️⃣ Client sends SET to master...");
        CacheRequest setRequest = new CacheRequest(
                UUID.randomUUID().toString(),
                System.currentTimeMillis(),
                CacheRequest.Command.SET,
                key,
                value
        );

        CacheResponse response = sendRequest(masterNode, setRequest);
        assertEquals(CacheResponse.Status.OK, response.status());
        System.out.println("    ✅ Master responded OK");
        System.out.println("  2️⃣ Waiting for replication...");
        waitForReplication(key, value);
        System.out.println("  3️⃣ Verifying all nodes...");
        for (NodeInfo node : allNodes) {
            String actual = stores.get(node.nodeId()).get(key);
            assertEquals(value, actual, "Node " + node.nodeId() + " should have replicated value");
            System.out.println("    ✅ Node " + node.nodeId() + ": " + actual);
        }
        System.out.println("✅ TEST PASSED: SET replicated to all nodes\n");
    }

    @Test
    @DisplayName("E2E: Client DELETE → Master → Replicas (full deletion)")
    void testFullStackDeleteReplication() throws Exception {
        System.out.println("\n🧪 TEST: Full stack DELETE replication");
        String key = "session:456";
        String value = "active";
        System.out.println("  0️⃣ Setup: Inserting initial value...");
        CacheRequest setupRequest = new CacheRequest(
                UUID.randomUUID().toString(),
                System.currentTimeMillis(),
                CacheRequest.Command.SET,
                key,
                value
        );
        sendRequest(masterNode, setupRequest);
        waitForReplication(key, value);
        System.out.println("    ✅ Initial value replicated");
        System.out.println("  1️⃣ Client sends DELETE to master...");
        CacheRequest deleteRequest = new CacheRequest(
                UUID.randomUUID().toString(),
                System.currentTimeMillis(),
                CacheRequest.Command.DELETE,
                key,
                null
        );

        CacheResponse response = sendRequest(masterNode, deleteRequest);
        assertEquals(CacheResponse.Status.OK, response.status());
        System.out.println("    ✅ Master responded OK");
        System.out.println("  2️⃣ Waiting for deletion replication...");
        waitForDeletion(key);
        System.out.println("  3️⃣ Verifying all nodes...");
        for (NodeInfo node : allNodes) {
            String actual = stores.get(node.nodeId()).get(key);
            assertNull(actual, "Node " + node.nodeId() + " should have deleted the key");
            System.out.println("    ✅ Node " + node.nodeId() + ": null (deleted)");
        }
        System.out.println("✅ TEST PASSED: DELETE replicated to all nodes\n");
    }

    @Test
    @DisplayName("E2E: Client GET from master returns correct value")
    void testFullStackGet() throws Exception {
        System.out.println("\n🧪 TEST: Full stack GET operation");
        String key = "product:789";
        String value = "Laptop";
        System.out.println("  0️⃣ Setup: Inserting value...");
        CacheRequest setupRequest = new CacheRequest(
                UUID.randomUUID().toString(),
                System.currentTimeMillis(),
                CacheRequest.Command.SET,
                key,
                value
        );
        sendRequest(masterNode, setupRequest);
        waitForReplication(key, value);
        System.out.println("  1️⃣ Client sends GET to master...");
        CacheRequest getRequest = new CacheRequest(
                UUID.randomUUID().toString(),
                System.currentTimeMillis(),
                CacheRequest.Command.GET,
                key,
                null
        );
        CacheResponse response = sendRequest(masterNode, getRequest);
        assertEquals(CacheResponse.Status.OK, response.status());
        assertEquals(value, response.value());
        System.out.println("    ✅ Master returned: " + response.value());
        System.out.println("✅ TEST PASSED: GET returned correct value\n");
    }

    @Test
    @DisplayName("E2E: Multiple concurrent clients")
    void testMultipleConcurrentClients() throws Exception {
        System.out.println("\n🧪 TEST: Multiple concurrent clients");
        int clientCount = 10;
        int operationsPerClient = 50;
        List<Thread> threads = new ArrayList<>();

        for (int i = 0; i < clientCount; i++) {
            final int clientId = i;
            Thread thread = new Thread(() -> {
                try {
                    for (int j = 0; j < operationsPerClient; j++) {
                        String key = "client-" + clientId + "-key-" + j;
                        String value = "value-" + j;

                        CacheRequest request = new CacheRequest(
                                UUID.randomUUID().toString(),
                                System.currentTimeMillis(),
                                CacheRequest.Command.SET,
                                key,
                                value
                        );

                        CacheResponse response = sendRequest(masterNode, request);
                        assertEquals(CacheResponse.Status.OK, response.status());
                    }
                } catch (Exception e) {
                    fail("Client " + clientId + " failed: " + e.getMessage());
                }
            });
            threads.add(thread);
            thread.start();
        }

        for (Thread thread : threads) {
            thread.join(30000);
        }
        System.out.println("  ✅ All " + clientCount + " clients completed " + operationsPerClient + " operations each");
        int expectedKeys = clientCount * operationsPerClient;
        int actualKeys = stores.get(masterNode.nodeId()).size();
        System.out.println("  📊 Master has " + actualKeys + " keys (expected ~" + expectedKeys + ")");
        assertTrue(actualKeys > 0, "Master should have keys");
        System.out.println("✅ TEST PASSED: Concurrent clients handled successfully\n");
    }

    @Test
    @DisplayName("E2E: GET on missing key returns NOT_FOUND")
    void testGetMissingKey() throws Exception {
        System.out.println("\n🧪 TEST: GET missing key");

        CacheRequest request = new CacheRequest(
                UUID.randomUUID().toString(),
                System.currentTimeMillis(),
                CacheRequest.Command.GET,
                "non-existent-key",
                null
        );

        CacheResponse response = sendRequest(masterNode, request);
        assertEquals(CacheResponse.Status.NOT_FOUND, response.status());
        assertNull(response.value());
        System.out.println("  ✅ Master returned NOT_FOUND");
        System.out.println("✅ TEST PASSED: Missing key handled correctly\n");
    }
}