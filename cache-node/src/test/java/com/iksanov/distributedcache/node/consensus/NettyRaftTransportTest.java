package com.iksanov.distributedcache.node.consensus;

import com.iksanov.distributedcache.node.consensus.model.*;
import com.iksanov.distributedcache.node.consensus.transport.NettyRaftTransport;
import com.iksanov.distributedcache.node.metrics.RaftMetrics;
import org.junit.jupiter.api.*;
import org.mockito.ArgumentCaptor;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Comprehensive integration tests for {@link NettyRaftTransport}.
 * <p>
 * Coverage:
 * - Server startup and shutdown
 * - VoteRequest/VoteResponse round-trip
 * - HeartbeatRequest/HeartbeatResponse round-trip
 * - Timeout handling
 * - Connection management and reconnection
 * - Concurrent requests
 * - Error handling
 * - Backpressure (MAX_PENDING_REQUESTS)
 * - Shutdown during active requests
 */
@DisplayName("NettyRaftTransport Integration Tests")
class NettyRaftTransportTest {
    private static final int BASE_PORT = 17000;
    private static final Duration TEST_TIMEOUT = Duration.ofSeconds(5);
    private RaftNode mockRaftNode;
    private RaftMetrics mockMetrics;
    private NettyRaftTransport transport;
    private int testPort;

    @BeforeEach
    void setUp() {
        mockRaftNode = mock(RaftNode.class);
        mockMetrics = mock(RaftMetrics.class);
        testPort = BASE_PORT + ThreadLocalRandom.current().nextInt(1000);
    }

    @AfterEach
    void tearDown() {
        if (transport != null) transport.shutdown();
    }

    @Test
    @DisplayName("Should start server successfully")
    void shouldStartServerSuccessfully() throws Exception {
        transport = new NettyRaftTransport(testPort, mockRaftNode, mockMetrics);
        assertDoesNotThrow(() -> transport.startServer());
        Thread.sleep(100);
    }

    @Test
    @DisplayName("Should shutdown gracefully")
    void shouldShutdownGracefully() throws Exception {
        transport = new NettyRaftTransport(testPort, mockRaftNode, mockMetrics);
        transport.startServer();
        assertDoesNotThrow(() -> transport.shutdown());
    }

    @Test
    @DisplayName("Should handle multiple shutdown calls")
    void shouldHandleMultipleShutdownCalls() throws Exception {
        transport = new NettyRaftTransport(testPort, mockRaftNode, mockMetrics);
        transport.startServer();
        transport.shutdown();
        assertDoesNotThrow(() -> transport.shutdown());
    }

    @Test
    @DisplayName("Should successfully send and receive VoteRequest")
    void shouldSendAndReceiveVoteRequest() throws Exception {
        int serverPort = testPort;
        int clientPort = testPort + 1;
        RaftNode serverNode = mock(RaftNode.class);
        RaftNode clientNode = mock(RaftNode.class);
        NettyRaftTransport server = new NettyRaftTransport(serverPort, serverNode, new RaftMetrics());
        NettyRaftTransport client = new NettyRaftTransport(clientPort, clientNode, new RaftMetrics());
        try {
            server.startServer();
            client.startServer();
            VoteResponse expectedResponse = new VoteResponse(5L, true);
            when(serverNode.handleVoteRequest(any(VoteRequest.class))).thenReturn(expectedResponse);
            VoteRequest request = new VoteRequest(5L, "client-node");
            CompletableFuture<VoteResponse> future = client.requestVote(
                    "localhost:" + serverPort,
                    request,
                    Duration.ofMillis(1000)
            );
            VoteResponse response = future.get(2, TimeUnit.SECONDS);
            assertNotNull(response);
            assertEquals(5L, response.term());
            assertTrue(response.voteGranted());
            ArgumentCaptor<VoteRequest> captor = ArgumentCaptor.forClass(VoteRequest.class);
            verify(serverNode, timeout(1000)).handleVoteRequest(captor.capture());
            assertEquals(5L, captor.getValue().term());
            assertEquals("client-node", captor.getValue().candidateId());
        } finally {
            server.shutdown();
            client.shutdown();
        }
    }

    @Test
    @DisplayName("Should handle VoteRequest with denied vote")
    void shouldHandleVoteRequestDenied() throws Exception {
        int serverPort = testPort;
        int clientPort = testPort + 1;
        RaftNode serverNode = mock(RaftNode.class);
        RaftNode clientNode = mock(RaftNode.class);
        NettyRaftTransport server = new NettyRaftTransport(serverPort, serverNode, new RaftMetrics());
        NettyRaftTransport client = new NettyRaftTransport(clientPort, clientNode, new RaftMetrics());
        try {
            server.startServer();
            client.startServer();
            VoteResponse deniedResponse = new VoteResponse(10L, false);
            when(serverNode.handleVoteRequest(any())).thenReturn(deniedResponse);
            VoteRequest request = new VoteRequest(5L, "client");
            CompletableFuture<VoteResponse> future = client.requestVote(
                    "localhost:" + serverPort, request, Duration.ofSeconds(1)
            );
            VoteResponse response = future.get(2, TimeUnit.SECONDS);
            assertNotNull(response);
            assertEquals(10L, response.term());
            assertFalse(response.voteGranted());
        } finally {
            server.shutdown();
            client.shutdown();
        }
    }

    @Test
    @DisplayName("Should successfully send and receive Heartbeat")
    void shouldSendAndReceiveHeartbeat() throws Exception {
        int serverPort = testPort;
        int clientPort = testPort + 1;
        RaftNode serverNode = mock(RaftNode.class);
        RaftNode clientNode = mock(RaftNode.class);
        NettyRaftTransport server = new NettyRaftTransport(serverPort, serverNode, new RaftMetrics());
        NettyRaftTransport client = new NettyRaftTransport(clientPort, clientNode, new RaftMetrics());
        try {
            server.startServer();
            client.startServer();
            HeartbeatResponse expectedResponse = new HeartbeatResponse(7L, true);
            when(serverNode.handleHeartbeat(any())).thenReturn(expectedResponse);
            HeartbeatRequest request = new HeartbeatRequest(7L, "leader-node");
            CompletableFuture<HeartbeatResponse> future = client.sendHeartbeat(
                    "localhost:" + serverPort, request, Duration.ofMillis(1000)
            );
            HeartbeatResponse response = future.get(2, TimeUnit.SECONDS);
            assertNotNull(response);
            assertEquals(7L, response.term());
            assertTrue(response.success());
            verify(serverNode, timeout(1000)).handleHeartbeat(any());
        } finally {
            server.shutdown();
            client.shutdown();
        }
    }

    @Test
    @DisplayName("Should handle Heartbeat failure")
    void shouldHandleHeartbeatFailure() throws Exception {
        int serverPort = testPort;
        int clientPort = testPort + 1;
        RaftNode serverNode = mock(RaftNode.class);
        RaftNode clientNode = mock(RaftNode.class);
        NettyRaftTransport server = new NettyRaftTransport(serverPort, serverNode, new RaftMetrics());
        NettyRaftTransport client = new NettyRaftTransport(clientPort, clientNode, new RaftMetrics());
        try {
            server.startServer();
            client.startServer();
            HeartbeatResponse failureResponse = new HeartbeatResponse(15L, false);
            when(serverNode.handleHeartbeat(any())).thenReturn(failureResponse);
            HeartbeatRequest request = new HeartbeatRequest(10L, "leader");
            CompletableFuture<HeartbeatResponse> future = client.sendHeartbeat(
                    "localhost:" + serverPort, request, Duration.ofSeconds(1)
            );
            HeartbeatResponse response = future.get(2, TimeUnit.SECONDS);
            assertNotNull(response);
            assertEquals(15L, response.term());
            assertFalse(response.success());
        } finally {
            server.shutdown();
            client.shutdown();
        }
    }

    @Test
    @DisplayName("Should timeout VoteRequest when no response")
    void shouldTimeoutVoteRequest() throws Exception {
        int serverPort = testPort;
        int clientPort = testPort + 1;
        RaftNode serverNode = mock(RaftNode.class);
        RaftNode clientNode = mock(RaftNode.class);
        when(serverNode.handleVoteRequest(any())).thenAnswer(invocation -> {
            Thread.sleep(5000);
            return new VoteResponse(1L, true);
        });
        NettyRaftTransport server = new NettyRaftTransport(serverPort, serverNode, new RaftMetrics());
        NettyRaftTransport client = new NettyRaftTransport(clientPort, clientNode, new RaftMetrics());
        try {
            server.startServer();
            client.startServer();
            VoteRequest request = new VoteRequest(1L, "client");
            CompletableFuture<VoteResponse> future = client.requestVote(
                    "localhost:" + serverPort, request, Duration.ofMillis(100)
            );
            ExecutionException ex = assertThrows(ExecutionException.class, () -> future.get(2, TimeUnit.SECONDS));
            assertInstanceOf(TimeoutException.class, ex.getCause());
        } finally {
            server.shutdown();
            client.shutdown();
        }
    }

    @Test
    @DisplayName("Should timeout Heartbeat when no response")
    void shouldTimeoutHeartbeat() throws Exception {
        int serverPort = testPort;
        int clientPort = testPort + 1;
        RaftNode serverNode = mock(RaftNode.class);
        RaftNode clientNode = mock(RaftNode.class);
        when(serverNode.handleHeartbeat(any())).thenAnswer(invocation -> {
            Thread.sleep(5000);
            return new HeartbeatResponse(1L, true);
        });
        NettyRaftTransport server = new NettyRaftTransport(serverPort, serverNode, new RaftMetrics());
        NettyRaftTransport client = new NettyRaftTransport(clientPort, clientNode, new RaftMetrics());
        try {
            server.startServer();
            client.startServer();
            HeartbeatRequest request = new HeartbeatRequest(1L, "leader");
            CompletableFuture<HeartbeatResponse> future = client.sendHeartbeat(
                    "localhost:" + serverPort, request, Duration.ofMillis(100)
            );
            ExecutionException ex = assertThrows(ExecutionException.class, () -> future.get(2, TimeUnit.SECONDS));
            assertInstanceOf(TimeoutException.class, ex.getCause());
        } finally {
            server.shutdown();
            client.shutdown();
        }
    }

    @Test
    @DisplayName("Should handle concurrent requests")
    void shouldHandleConcurrentRequests() throws Exception {
        int serverPort = testPort;
        int clientPort = testPort + 1;
        RaftNode serverNode = mock(RaftNode.class);
        RaftNode clientNode = mock(RaftNode.class);
        NettyRaftTransport server = new NettyRaftTransport(serverPort, serverNode, new RaftMetrics());
        NettyRaftTransport client = new NettyRaftTransport(clientPort, clientNode, new RaftMetrics());
        try {
            server.startServer();
            client.startServer();
            when(serverNode.handleVoteRequest(any())).thenAnswer(invocation -> {
                VoteRequest req = invocation.getArgument(0);
                return new VoteResponse(req.term(), req.term() % 2 == 0);
            });
            int numRequests = 10;
            List<CompletableFuture<VoteResponse>> futures = new ArrayList<>();
            for (int i = 0; i < numRequests; i++) {
                VoteRequest req = new VoteRequest(i, "client-" + i);
                CompletableFuture<VoteResponse> future = client.requestVote(
                        "localhost:" + serverPort, req, Duration.ofSeconds(2)
                );
                futures.add(future);
            }
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get(5, TimeUnit.SECONDS);
            for (int i = 0; i < numRequests; i++) {
                VoteResponse resp = futures.get(i).get();
                assertNotNull(resp);
                assertEquals(i, resp.term());
                assertEquals(i % 2 == 0, resp.voteGranted());
            }
        } finally {
            server.shutdown();
            client.shutdown();
        }
    }

    @Test
    @DisplayName("Should reject requests when exceeding MAX_PENDING_REQUESTS")
    void shouldRejectWhenExceedingMaxPending() throws Exception {
        int serverPort = testPort;
        int clientPort = testPort + 1;
        RaftNode serverNode = mock(RaftNode.class);
        RaftNode clientNode = mock(RaftNode.class);
        when(serverNode.handleVoteRequest(any())).thenAnswer(invocation -> {
            Thread.sleep(10000);
            return new VoteResponse(1L, true);
        });
        NettyRaftTransport server = new NettyRaftTransport(serverPort, serverNode, new RaftMetrics());
        NettyRaftTransport client = new NettyRaftTransport(clientPort, clientNode, new RaftMetrics());
        try {
            server.startServer();
            client.startServer();
            List<CompletableFuture<VoteResponse>> futures = new ArrayList<>();
            for (int i = 0; i < 1100; i++) {
                VoteRequest req = new VoteRequest(i, "node-" + i);
                CompletableFuture<VoteResponse> future = client.requestVote(
                        "localhost:" + serverPort,
                        req,
                        Duration.ofSeconds(30)
                );
                futures.add(future);
            }
            Thread.sleep(100);
            int rejectedCount = 0;
            for (CompletableFuture<VoteResponse> future : futures) {
                if (future.isCompletedExceptionally()) {
                    try {
                        future.get();
                    } catch (ExecutionException e) {
                        if (e.getCause() != null &&
                                e.getCause().getMessage().contains("Too many pending requests")) {
                            rejectedCount++;
                        }
                    }
                }
            }
            assertTrue(rejectedCount > 0,
                    "Expected some requests to be rejected due to backpressure. " +
                            "Rejected: " + rejectedCount + " out of 1100");
        } finally {
            server.shutdown();
            client.shutdown();
        }
    }

    @Test
    @DisplayName("Should reject new requests after shutdown")
    void shouldRejectRequestsAfterShutdown() throws Exception {
        transport = new NettyRaftTransport(testPort, mockRaftNode, mockMetrics);
        transport.startServer();
        transport.shutdown();
        VoteRequest request = new VoteRequest(1L, "node");
        CompletableFuture<VoteResponse> future = transport.requestVote(
                "localhost:" + (testPort + 1), request, Duration.ofMillis(100)
        );
        ExecutionException ex = assertThrows(ExecutionException.class, () -> future.get(1, TimeUnit.SECONDS));
        assertTrue(ex.getCause().getMessage().contains("shutting down"));
    }

    @Test
    @DisplayName("Should complete pending requests exceptionally on shutdown")
    void shouldCompleteExceptionallyOnShutdown() throws Exception {
        int serverPort = testPort;
        int clientPort = testPort + 1;
        RaftNode serverNode = mock(RaftNode.class);
        RaftNode clientNode = mock(RaftNode.class);
        when(serverNode.handleVoteRequest(any())).thenAnswer(invocation -> {
            Thread.sleep(5000);
            return new VoteResponse(1L, true);
        });
        NettyRaftTransport server = new NettyRaftTransport(serverPort, serverNode, new RaftMetrics());
        NettyRaftTransport client = new NettyRaftTransport(clientPort, clientNode, new RaftMetrics());
        try {
            server.startServer();
            client.startServer();
            VoteRequest request = new VoteRequest(1L, "client");
            CompletableFuture<VoteResponse> future = client.requestVote(
                    "localhost:" + serverPort, request, Duration.ofSeconds(10)
            );
            Thread.sleep(100);
            client.shutdown();
            ExecutionException ex = assertThrows(ExecutionException.class, () -> future.get(1, TimeUnit.SECONDS));
            assertTrue(ex.getCause().getMessage().contains("shutdown") || ex.getCause().getMessage().contains("Transport"));
        } finally {
            server.shutdown();
        }
    }

    @Test
    @DisplayName("Should handle null timeout gracefully")
    void shouldHandleNullTimeout() throws Exception {
        int serverPort = testPort;
        int clientPort = testPort + 1;
        RaftNode serverNode = mock(RaftNode.class);
        RaftNode clientNode = mock(RaftNode.class);
        NettyRaftTransport server = new NettyRaftTransport(serverPort, serverNode, new RaftMetrics());
        NettyRaftTransport client = new NettyRaftTransport(clientPort, clientNode, new RaftMetrics());
        try {
            server.startServer();
            client.startServer();
            VoteResponse response = new VoteResponse(1L, true);
            when(serverNode.handleVoteRequest(any())).thenReturn(response);
            VoteRequest request = new VoteRequest(1L, "client");
            CompletableFuture<VoteResponse> future = client.requestVote(
                    "localhost:" + serverPort,
                    request,
                    null
            );
            VoteResponse resp = future.get(2, TimeUnit.SECONDS);
            assertNotNull(resp);
        } finally {
            server.shutdown();
            client.shutdown();
        }
    }

    @Test
    @DisplayName("Should handle server handler exception gracefully")
    void shouldHandleServerExceptionGracefully() throws Exception {
        int serverPort = testPort;
        int clientPort = testPort + 1;
        RaftNode serverNode = mock(RaftNode.class);
        RaftNode clientNode = mock(RaftNode.class);
        when(serverNode.handleVoteRequest(any())).thenThrow(new RuntimeException("Server error"));
        NettyRaftTransport server = new NettyRaftTransport(serverPort, serverNode, new RaftMetrics());
        NettyRaftTransport client = new NettyRaftTransport(clientPort, clientNode, new RaftMetrics());
        try {
            server.startServer();
            client.startServer();
            VoteRequest request = new VoteRequest(1L, "client");
            CompletableFuture<VoteResponse> future = client.requestVote(
                    "localhost:" + serverPort, request, Duration.ofMillis(500)
            );
            assertThrows(Exception.class, () -> future.get(2, TimeUnit.SECONDS));
        } finally {
            server.shutdown();
            client.shutdown();
        }
    }
}