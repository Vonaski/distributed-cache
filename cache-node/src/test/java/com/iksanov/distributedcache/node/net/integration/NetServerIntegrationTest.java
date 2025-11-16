package com.iksanov.distributedcache.node.net.integration;

import com.iksanov.distributedcache.common.codec.CacheMessageCodec;
import com.iksanov.distributedcache.common.dto.CacheRequest;
import com.iksanov.distributedcache.common.dto.CacheResponse;
import com.iksanov.distributedcache.node.config.NetServerConfig;
import com.iksanov.distributedcache.node.consensus.model.Command;
import com.iksanov.distributedcache.node.consensus.raft.RaftNode;
import com.iksanov.distributedcache.node.consensus.raft.RaftStateMachine;
import com.iksanov.distributedcache.node.consensus.sharding.ShardManager;
import com.iksanov.distributedcache.node.core.InMemoryCacheStore;
import com.iksanov.distributedcache.node.metrics.NetMetrics;
import com.iksanov.distributedcache.node.net.NetServer;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import org.junit.jupiter.api.*;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Integration tests for {@link NetServer}.
 *
 * <p>Starts a real {@link NetServer} backed by {@link InMemoryCacheStore}.
 * Then connects real Netty clients using {@link CacheMessageCodec} to verify
 * full request/response flow (SET/GET) and concurrent client handling.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class NetServerIntegrationTest {

    private static final String HOST = "127.0.0.1";
    private static final int PORT = 7001;
    private static final String SHARD_ID = "shard-0";
    private NetServer server;
    private NioEventLoopGroup clientGroup;
    private Bootstrap clientBootstrap;
    private final Map<String, CompletableFuture<CacheResponse>> responseMap = new ConcurrentHashMap<>();
    private final NetMetrics netMetrics = new NetMetrics();

    // Test storage - simulates committed state
    private final Map<String, String> testStorage = new ConcurrentHashMap<>();
    private ShardManager mockShardManager;
    private RaftNode mockRaftNode;
    private RaftStateMachine mockStateMachine;

    @BeforeAll
    void setUp() {
        // Create mock ShardManager
        mockShardManager = mock(ShardManager.class);
        mockRaftNode = mock(RaftNode.class);
        mockStateMachine = mock(RaftStateMachine.class);

        // ShardManager always returns the same shard for any key
        when(mockShardManager.selectShardForKey(any())).thenReturn(SHARD_ID);
        when(mockShardManager.getShard(SHARD_ID)).thenReturn(mockRaftNode);
        when(mockShardManager.getStateMachine(SHARD_ID)).thenReturn(mockStateMachine);
        when(mockShardManager.localShardIds()).thenReturn(Set.of(SHARD_ID));

        // Mock RaftNode.submit() to immediately commit and store the command
        when(mockRaftNode.submit(any(Command.class))).thenAnswer(invocation -> {
            Command cmd = invocation.getArgument(0);
            if (cmd.type() == Command.Type.SET) {
                testStorage.put(cmd.key(), cmd.value());
            } else if (cmd.type() == Command.Type.DELETE) {
                testStorage.remove(cmd.key());
            }
            return CompletableFuture.completedFuture(1L);
        });

        // Mock RaftStateMachine.get() to read from test storage
        when(mockStateMachine.get(any())).thenAnswer(invocation -> {
            String key = invocation.getArgument(0);
            return testStorage.get(key);
        });

        NetServerConfig config = new NetServerConfig(
                HOST,
                PORT,
                1,
                1,
                128,
                10 * 1024 * 1024,
                2,
                10,
                100L,
                10L
        );

        server = new NetServer(config, mockShardManager, netMetrics);
        server.start();

        // Give server time to start
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        clientGroup = new NioEventLoopGroup();
        clientBootstrap = createClientBootstrap(clientGroup);
    }

    @BeforeEach
    void clearStorage() {
        testStorage.clear();
    }

    @AfterAll
    void tearDown() throws InterruptedException {
        if (server != null) {
            server.stop();
        }
        if (clientGroup != null) {
            clientGroup.shutdownGracefully().sync();
        }
    }

    private Bootstrap createClientBootstrap(EventLoopGroup group) {
        return new Bootstrap()
                .group(group)
                .channel(NioSocketChannel.class)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ChannelPipeline p = ch.pipeline();
                        p.addLast("frameDecoder", new LengthFieldBasedFrameDecoder(
                                10 * 1024 * 1024, 0, 4, 0, 4));
                        p.addLast("framePrepender", new LengthFieldPrepender(4));
                        p.addLast("codec", new CacheMessageCodec());
                        p.addLast("clientHandler", new SimpleChannelInboundHandler<CacheResponse>() {
                            @Override
                            protected void channelRead0(ChannelHandlerContext ctx, CacheResponse response) {
                                CompletableFuture<CacheResponse> future = responseMap.remove(response.requestId());
                                if (future != null) {
                                    future.complete(response);
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

    private CacheResponse sendRequest(CacheRequest request) throws Exception {
        CompletableFuture<CacheResponse> future = new CompletableFuture<>();
        responseMap.put(request.requestId(), future);

        Channel channel = clientBootstrap.connect(HOST, PORT).sync().channel();
        channel.writeAndFlush(request).sync();

        CacheResponse response = future.get(5, TimeUnit.SECONDS);
        channel.close().sync();
        return response;
    }

    @Test
    @Order(1)
    @DisplayName("Should perform end-to-end SET and GET successfully")
    void shouldPerformEndToEndSetAndGet() throws Exception {
        String key = "keyA";
        String value = "valueA";

        CacheRequest setReq = new CacheRequest(
                UUID.randomUUID().toString(),
                System.currentTimeMillis(),
                CacheRequest.Command.SET,
                key,
                value
        );

        CacheResponse setResp = sendRequest(setReq);
        assertEquals(CacheResponse.Status.OK, setResp.status());
        assertNull(setResp.errorMessage(), "SET should not return error");

        CacheRequest getReq = new CacheRequest(
                UUID.randomUUID().toString(),
                System.currentTimeMillis(),
                CacheRequest.Command.GET,
                key,
                null
        );

        CacheResponse getResp = sendRequest(getReq);
        assertEquals(CacheResponse.Status.OK, getResp.status());
        assertEquals(value, getResp.value(), "Returned value must match stored one");
    }

    @Test
    @Order(2)
    @DisplayName("Should handle concurrent client requests correctly")
    void shouldHandleConcurrentClients() throws Exception {
        int clients = 8;
        ExecutorService pool = Executors.newFixedThreadPool(clients);
        CountDownLatch latch = new CountDownLatch(clients);

        for (int i = 0; i < clients; i++) {
            final int idx = i;
            pool.submit(() -> {
                try {
                    String key = "key-" + idx;
                    String value = "value-" + idx;
                    CacheRequest req = new CacheRequest(
                            UUID.randomUUID().toString(),
                            System.currentTimeMillis(),
                            CacheRequest.Command.SET,
                            key,
                            value
                    );

                    CacheResponse resp = sendRequest(req);
                    assertEquals(CacheResponse.Status.OK, resp.status(), "SET should succeed for client " + idx);
                } catch (Exception e) {
                    e.printStackTrace();
                    fail("Client " + idx + " failed: " + e.getMessage());
                } finally {
                    latch.countDown();
                }
            });
        }

        boolean completed = latch.await(10, TimeUnit.SECONDS);
        pool.shutdownNow();
        assertTrue(completed, "All clients must complete within timeout");
    }
}
