package com.iksanov.distributedcache.node.consensus;

import com.iksanov.distributedcache.node.config.RaftConfig;
import com.iksanov.distributedcache.node.consensus.model.*;
import com.iksanov.distributedcache.node.consensus.persistence.RaftPersistence;
import com.iksanov.distributedcache.node.metrics.RaftMetrics;
import org.junit.jupiter.api.*;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Comprehensive unit tests for {@link RaftNode}.
 * <p>
 * Coverage:
 * - Node initialization and state restoration
 * - Leader election (single node, multiple nodes, split votes)
 * - Vote request handling (grant/deny)
 * - Heartbeat mechanism (sending/receiving)
 * - Step down on higher term
 * - Persistence integration
 * - Leader change listeners
 * - Graceful shutdown
 * - Edge cases and error handling
 */
@DisplayName("RaftNode Unit Tests")
class RaftNodeTest {

    private static final int BASE_PORT = 18000;
    private RaftPersistence mockPersistence;
    private RaftMetrics mockMetrics;
    private RaftConfig testConfig;
    private int testPort;

    @BeforeEach
    void setUp() throws IOException {
        mockPersistence = mock(RaftPersistence.class);
        mockMetrics = mock(RaftMetrics.class);
        testPort = BASE_PORT + ThreadLocalRandom.current().nextInt(1000);
        testConfig = new RaftConfig(50, 100, 20, 100);
        when(mockPersistence.loadTerm()).thenReturn(0L);
        when(mockPersistence.loadVotedFor()).thenReturn(null);
        doNothing().when(mockPersistence).saveTerm(anyLong());
        doNothing().when(mockPersistence).saveVotedFor(anyString());
    }

    @Test
    @DisplayName("Should initialize as FOLLOWER")
    void shouldInitializeAsFollower() {
        List<String> nodes = Arrays.asList("node-A", "node-B");
        RaftNode node = new RaftNode("node-A", nodes, testConfig, mockPersistence, mockMetrics, testPort);

        try {
            assertEquals(RaftNode.NodeState.FOLLOWER, node.getState());
            assertEquals(0L, node.getCurrentTerm());
            assertNull(node.getCurrentLeader());
        } finally {
            node.shutdown();
        }
    }

    @Test
    @DisplayName("Should restore state from persistence")
    void shouldRestoreStateFromPersistence() throws IOException {
        when(mockPersistence.loadTerm()).thenReturn(5L);
        when(mockPersistence.loadVotedFor()).thenReturn("node-B");
        List<String> nodes = Arrays.asList("node-A", "node-B");
        RaftNode node = new RaftNode("node-A", nodes, testConfig, mockPersistence, mockMetrics, testPort);

        try {
            assertEquals(5L, node.getCurrentTerm());
            verify(mockPersistence).loadTerm();
            verify(mockPersistence).loadVotedFor();
        } finally {
            node.shutdown();
        }
    }

    @Test
    @DisplayName("Should handle persistence load failure gracefully")
    void shouldHandlePersistenceLoadFailure() throws IOException {
        when(mockPersistence.loadTerm()).thenThrow(new IOException("Disk error"));
        List<String> nodes = Arrays.asList("node-A", "node-B");
        RaftNode node = new RaftNode("node-A", nodes, testConfig, mockPersistence, mockMetrics, testPort);
        try {
            assertEquals(0L, node.getCurrentTerm());
        } finally {
            node.shutdown();
        }
    }

    @Test
    @DisplayName("Should use default config when null provided")
    void shouldUseDefaultConfig() {
        List<String> nodes = Arrays.asList("node-A", "node-B");
        RaftNode node = new RaftNode("node-A", nodes, null, mockPersistence, mockMetrics, testPort);
        try {
            assertNotNull(node);
            assertEquals(RaftNode.NodeState.FOLLOWER, node.getState());
        } finally {
            node.shutdown();
        }
    }

    @Test
    @DisplayName("Single node should immediately become leader")
    void singleNodeShouldBecomeLeader() throws Exception {
        List<String> nodes = Collections.singletonList("node-A");
        RaftNode node = new RaftNode("node-A", nodes, testConfig, mockPersistence, mockMetrics, testPort);

        try {
            Thread.sleep(testConfig.electionTimeoutMaxMs() + 100);
            assertEquals(RaftNode.NodeState.LEADER, node.getState());
            assertEquals("node-A", node.getCurrentLeader());
            assertTrue(node.getCurrentTerm() > 0);
            verify(mockMetrics, atLeastOnce()).incrementElectionsStarted(anyLong());
            verify(mockMetrics, atLeastOnce()).incrementLeadersElected(anyLong());
        } finally {
            node.shutdown();
        }
    }

    @Test
    @DisplayName("Should grant vote to first candidate in same term")
    void shouldGrantVoteToFirstCandidate() {
        List<String> nodes = Arrays.asList("node-A", "node-B");
        RaftNode node = new RaftNode("node-A", nodes, testConfig, mockPersistence, mockMetrics, testPort);

        try {
            VoteRequest request = new VoteRequest(1L, "node-B");
            VoteResponse response = node.handleVoteRequest(request);
            assertNotNull(response);
            assertTrue(response.voteGranted);
            assertEquals(1L, node.getCurrentTerm());
            verify(mockMetrics).incrementVoteGranted();
        } finally {
            node.shutdown();
        }
    }

    @Test
    @DisplayName("Should deny vote if already voted for another candidate")
    void shouldDenyVoteIfAlreadyVoted() {
        List<String> nodes = Arrays.asList("node-A", "node-B", "node-C");
        RaftNode node = new RaftNode("node-A", nodes, testConfig, mockPersistence, mockMetrics, testPort);

        try {
            VoteRequest request1 = new VoteRequest(1L, "node-B");
            VoteResponse response1 = node.handleVoteRequest(request1);
            assertTrue(response1.voteGranted);
            VoteRequest request2 = new VoteRequest(1L, "node-C");
            VoteResponse response2 = node.handleVoteRequest(request2);
            assertFalse(response2.voteGranted);
            verify(mockMetrics).incrementVoteGranted();
            verify(mockMetrics).incrementVoteDenied();
        } finally {
            node.shutdown();
        }
    }

    @Test
    @DisplayName("Should deny vote if request term is lower than current term")
    void shouldDenyVoteForOlderTerm() {
        List<String> nodes = Arrays.asList("node-A", "node-B");
        RaftNode node = new RaftNode("node-A", nodes, testConfig, mockPersistence, mockMetrics, testPort);

        try {
            VoteRequest request1 = new VoteRequest(5L, "node-B");
            node.handleVoteRequest(request1);
            VoteRequest request2 = new VoteRequest(3L, "node-B");
            VoteResponse response2 = node.handleVoteRequest(request2);
            assertFalse(response2.voteGranted);
            assertEquals(5L, response2.term);
        } finally {
            node.shutdown();
        }
    }

    @Test
    @DisplayName("Should step down when vote request has higher term")
    void shouldStepDownOnHigherTermVoteRequest() throws IOException {
        List<String> nodes = Arrays.asList("node-A", "node-B");
        RaftNode node = new RaftNode("node-A", nodes, testConfig, mockPersistence, mockMetrics, testPort);
        try {
            VoteRequest request = new VoteRequest(10L, "node-B");
            VoteResponse response = node.handleVoteRequest(request);
            assertEquals(10L, node.getCurrentTerm());
            assertTrue(response.voteGranted);
            verify(mockPersistence).saveTerm(10L);
        } finally {
            node.shutdown();
        }
    }

    @Test
    @DisplayName("Should allow voting for same candidate again")
    void shouldAllowVotingForSameCandidateAgain() {
        List<String> nodes = Arrays.asList("node-A", "node-B");
        RaftNode node = new RaftNode("node-A", nodes, testConfig, mockPersistence, mockMetrics, testPort);
        try {
            VoteRequest request1 = new VoteRequest(1L, "node-B");
            VoteResponse response1 = node.handleVoteRequest(request1);
            assertTrue(response1.voteGranted);
            VoteRequest request2 = new VoteRequest(1L, "node-B");
            VoteResponse response2 = node.handleVoteRequest(request2);
            assertTrue(response2.voteGranted);
        } finally {
            node.shutdown();
        }
    }

    @Test
    @DisplayName("Should accept heartbeat from leader")
    void shouldAcceptHeartbeatFromLeader() {
        List<String> nodes = Arrays.asList("node-A", "node-B");
        RaftNode node = new RaftNode("node-A", nodes, testConfig, mockPersistence, mockMetrics, testPort);

        try {
            HeartbeatRequest request = new HeartbeatRequest(1L, "node-B");
            HeartbeatResponse response = node.handleHeartbeat(request);
            assertNotNull(response);
            assertTrue(response.success);
            assertEquals(1L, node.getCurrentTerm());
            assertEquals("node-B", node.getCurrentLeader());
            verify(mockMetrics, atLeastOnce()).incrementHeartbeatsReceived();
        } finally {
            node.shutdown();
        }
    }

    @Test
    @DisplayName("Should reject heartbeat from older term")
    void shouldRejectHeartbeatFromOlderTerm() {
        List<String> nodes = Arrays.asList("node-A", "node-B");
        RaftNode node = new RaftNode("node-A", nodes, testConfig, mockPersistence, mockMetrics, testPort);

        try {
            HeartbeatRequest request1 = new HeartbeatRequest(5L, "node-B");
            node.handleHeartbeat(request1);
            HeartbeatRequest request2 = new HeartbeatRequest(3L, "node-B");
            HeartbeatResponse response2 = node.handleHeartbeat(request2);
            assertFalse(response2.success);
            assertEquals(5L, response2.term);
        } finally {
            node.shutdown();
        }
    }

    @Test
    @DisplayName("Should step down on heartbeat with higher term")
    void shouldStepDownOnHigherTermHeartbeat() throws IOException {
        List<String> nodes = Arrays.asList("node-A", "node-B");
        RaftNode node = new RaftNode("node-A", nodes, testConfig, mockPersistence, mockMetrics, testPort);

        try {
            HeartbeatRequest request = new HeartbeatRequest(15L, "node-B");
            HeartbeatResponse response = node.handleHeartbeat(request);
            assertEquals(15L, node.getCurrentTerm());
            assertTrue(response.success);
            assertEquals("node-B", node.getCurrentLeader());
            verify(mockPersistence).saveTerm(15L);
        } finally {
            node.shutdown();
        }
    }

    @Test
    @DisplayName("Should reset election timer on heartbeat")
    void shouldResetElectionTimerOnHeartbeat() throws Exception {
        List<String> nodes = Arrays.asList("node-A", "node-B");
        RaftNode node = new RaftNode("node-A", nodes, testConfig, mockPersistence, mockMetrics, testPort);

        try {
            Thread.sleep(testConfig.electionTimeoutMinMs() - 20);
            HeartbeatRequest request = new HeartbeatRequest(1L, "node-B");
            node.handleHeartbeat(request);
            Thread.sleep(40);
            assertEquals(RaftNode.NodeState.FOLLOWER, node.getState());
        } finally {
            node.shutdown();
        }
    }

    @Test
    @DisplayName("Should notify listener when becoming leader")
    void shouldNotifyListenerOnBecomeLeader() throws Exception {
        List<String> nodes = Collections.singletonList("node-A");
        RaftNode node = new RaftNode("node-A", nodes, testConfig, mockPersistence, mockMetrics, testPort);
        CountDownLatch latch = new CountDownLatch(1);
        LeaderChangeListener listener = mock(LeaderChangeListener.class);
        doAnswer(inv -> {
            latch.countDown();
            return null;
        }).when(listener).onBecomeLeader();

        try {
            node.addLeaderChangeListener(listener);
            assertTrue(latch.await(500, TimeUnit.MILLISECONDS), "Listener should be notified of leadership");
            verify(listener).onBecomeLeader();
        } finally {
            node.shutdown();
        }
    }

    @Test
    @DisplayName("Should notify listener when losing leadership")
    void shouldNotifyListenerOnLoseLeadership() throws Exception {
        List<String> nodes = Collections.singletonList("node-A");
        RaftNode node = new RaftNode("node-A", nodes, testConfig, mockPersistence, mockMetrics, testPort);
        LeaderChangeListener listener = mock(LeaderChangeListener.class);
        node.addLeaderChangeListener(listener);

        try {
            Thread.sleep(testConfig.electionTimeoutMaxMs() + 100);
            assertEquals(RaftNode.NodeState.LEADER, node.getState());
            VoteRequest request = new VoteRequest(100L, "node-B");
            node.handleVoteRequest(request);
            verify(listener).onLoseLeadership();
        } finally {
            node.shutdown();
        }
    }

    @Test
    @DisplayName("Should handle listener exception gracefully")
    void shouldHandleListenerExceptionGracefully() throws Exception {
        List<String> nodes = Collections.singletonList("node-A");
        RaftNode node = new RaftNode("node-A", nodes, testConfig, mockPersistence, mockMetrics, testPort);
        LeaderChangeListener listener = mock(LeaderChangeListener.class);
        doThrow(new RuntimeException("Listener error")).when(listener).onBecomeLeader();

        try {
            node.addLeaderChangeListener(listener);
            Thread.sleep(testConfig.electionTimeoutMaxMs() + 100);
            assertEquals(RaftNode.NodeState.LEADER, node.getState());
        } finally {
            node.shutdown();
        }
    }

    @Test
    @DisplayName("Should persist term on election")
    void shouldPersistTermOnElection() throws Exception {
        List<String> nodes = Collections.singletonList("node-A");
        RaftNode node = new RaftNode("node-A", nodes, testConfig, mockPersistence, mockMetrics, testPort);

        try {
            Thread.sleep(testConfig.electionTimeoutMaxMs() + 100);
            verify(mockPersistence, atLeastOnce()).saveTerm(anyLong());
            verify(mockPersistence, atLeastOnce()).saveVotedFor(eq("node-A"));
        } finally {
            node.shutdown();
        }
    }

    @Test
    @DisplayName("Should persist votedFor when granting vote")
    void shouldPersistVotedForOnVote() throws IOException {
        List<String> nodes = Arrays.asList("node-A", "node-B");
        RaftNode node = new RaftNode("node-A", nodes, testConfig, mockPersistence, mockMetrics, testPort);

        try {
            VoteRequest request = new VoteRequest(1L, "node-B");
            node.handleVoteRequest(request);
            verify(mockPersistence).saveVotedFor("node-B");
        } finally {
            node.shutdown();
        }
    }

    @Test
    @DisplayName("Should persist null votedFor on step down")
    void shouldPersistNullVotedForOnStepDown() throws IOException {
        List<String> nodes = Collections.singletonList("node-A");
        RaftNode node = new RaftNode("node-A", nodes, testConfig, mockPersistence, mockMetrics, testPort);

        try {
            Thread.sleep(testConfig.electionTimeoutMaxMs() + 100);
            assertEquals(RaftNode.NodeState.LEADER, node.getState());
            VoteRequest request = new VoteRequest(100L, "node-B");
            node.handleVoteRequest(request);
            verify(mockPersistence).saveVotedFor(null);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            node.shutdown();
        }
    }

    @Test
    @DisplayName("Should handle persistence save failure gracefully")
    void shouldHandlePersistenceSaveFailureGracefully() throws IOException {
        doThrow(new IOException("Disk full")).when(mockPersistence).saveTerm(anyLong());

        List<String> nodes = Collections.singletonList("node-A");
        RaftNode node = new RaftNode("node-A", nodes, testConfig, mockPersistence, mockMetrics, testPort);

        try {
            Thread.sleep(testConfig.electionTimeoutMaxMs() + 200);
            assertNotNull(node.getState());
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            node.shutdown();
        }
    }

    @Test
    @DisplayName("Should record election metrics")
    void shouldRecordElectionMetrics() throws Exception {
        List<String> nodes = Collections.singletonList("node-A");
        RaftNode node = new RaftNode("node-A", nodes, testConfig, mockPersistence, mockMetrics, testPort);

        try {
            Thread.sleep(testConfig.electionTimeoutMaxMs() + 100);
            verify(mockMetrics, atLeastOnce()).incrementElectionsStarted(anyLong());
            verify(mockMetrics, atLeastOnce()).incrementLeadersElected(anyLong());
        } finally {
            node.shutdown();
        }
    }

    @Test
    @DisplayName("Should record vote metrics")
    void shouldRecordVoteMetrics() {
        List<String> nodes = Arrays.asList("node-A", "node-B");
        RaftNode node = new RaftNode("node-A", nodes, testConfig, mockPersistence, mockMetrics, testPort);

        try {
            VoteRequest granted = new VoteRequest(1L, "node-B");
            node.handleVoteRequest(granted);
            verify(mockMetrics).incrementVoteGranted();
            VoteRequest denied = new VoteRequest(1L, "node-C");
            node.handleVoteRequest(denied);
            verify(mockMetrics).incrementVoteDenied();
        } finally {
            node.shutdown();
        }
    }

    @Test
    @DisplayName("Should record heartbeat metrics")
    void shouldRecordHeartbeatMetrics() {
        List<String> nodes = Arrays.asList("node-A", "node-B");
        RaftNode node = new RaftNode("node-A", nodes, testConfig, mockPersistence, mockMetrics, testPort);

        try {
            HeartbeatRequest request = new HeartbeatRequest(1L, "node-B");
            node.handleHeartbeat(request);
            verify(mockMetrics, atLeastOnce()).incrementHeartbeatsReceived();
        } finally {
            node.shutdown();
        }
    }

    @Test
    @DisplayName("Should shutdown gracefully")
    void shouldShutdownGracefully() {
        List<String> nodes = Arrays.asList("node-A", "node-B");
        RaftNode node = new RaftNode("node-A", nodes, testConfig, mockPersistence, mockMetrics, testPort);
        assertDoesNotThrow(() -> node.shutdown());
    }

    @Test
    @DisplayName("Should handle multiple shutdown calls")
    void shouldHandleMultipleShutdownCalls() {
        List<String> nodes = Arrays.asList("node-A", "node-B");
        RaftNode node = new RaftNode("node-A", nodes, testConfig, mockPersistence, mockMetrics, testPort);
        node.shutdown();
        assertDoesNotThrow(() -> node.shutdown());
    }

    @Test
    @DisplayName("Should not start election after shutdown")
    void shouldNotStartElectionAfterShutdown() throws Exception {
        List<String> nodes = Collections.singletonList("node-A");
        RaftNode node = new RaftNode("node-A", nodes, testConfig, mockPersistence, mockMetrics, testPort);
        node.shutdown();
        Thread.sleep(testConfig.electionTimeoutMaxMs() + 100);
        assertEquals(RaftNode.NodeState.FOLLOWER, node.getState());
    }

    @Test
    @DisplayName("Should handle empty peer list")
    void shouldHandleEmptyPeerList() {
        List<String> nodes = Collections.singletonList("node-A");
        RaftNode node = new RaftNode("node-A", nodes, testConfig, mockPersistence, mockMetrics, testPort);

        try {
            assertNotNull(node);
            assertEquals("node-A", node.getNodeId());
        } finally {
            node.shutdown();
        }
    }

    @Test
    @DisplayName("Should handle vote request exception gracefully")
    void shouldHandleVoteRequestExceptionGracefully() throws IOException {
        doThrow(new IOException("Persistence error")).when(mockPersistence).saveVotedFor(anyString());
        List<String> nodes = Arrays.asList("node-A", "node-B");
        RaftNode node = new RaftNode("node-A", nodes, testConfig, mockPersistence, mockMetrics, testPort);

        try {
            VoteRequest request = new VoteRequest(1L, "node-B");
            VoteResponse response = node.handleVoteRequest(request);
            assertNotNull(response);
            assertFalse(response.voteGranted);
        } finally {
            node.shutdown();
        }
    }

    @Test
    @DisplayName("Should handle heartbeat exception gracefully")
    void shouldHandleHeartbeatExceptionGracefully() throws IOException {
        doThrow(new IOException("Persistence error")).when(mockPersistence).saveTerm(anyLong());
        List<String> nodes = Arrays.asList("node-A", "node-B");
        RaftNode node = new RaftNode("node-A", nodes, testConfig, mockPersistence, mockMetrics, testPort);

        try {
            HeartbeatRequest request = new HeartbeatRequest(10L, "node-B");
            HeartbeatResponse response = node.handleHeartbeat(request);
            assertNotNull(response);
            assertFalse(response.success);
        } finally {
            node.shutdown();
        }
    }

    @Test
    @DisplayName("Should handle concurrent vote requests")
    void shouldHandleConcurrentVoteRequests() throws Exception {
        List<String> nodes = Arrays.asList("node-A", "node-B", "node-C");
        RaftNode node = new RaftNode("node-A", nodes, testConfig, mockPersistence, mockMetrics, testPort);

        try {
            CountDownLatch latch = new CountDownLatch(10);
            ExecutorService executor = Executors.newFixedThreadPool(10);
            for (int i = 0; i < 10; i++) {
                final int term = i;
                executor.submit(() -> {
                    try {
                        VoteRequest request = new VoteRequest(term, "node-B");
                        node.handleVoteRequest(request);
                    } finally {
                        latch.countDown();
                    }
                });
            }
            assertTrue(latch.await(2, TimeUnit.SECONDS));
            executor.shutdown();
        } finally {
            node.shutdown();
        }
    }

    @Test
    @DisplayName("Should handle term overflow gracefully")
    void shouldHandleTermOverflowGracefully() {
        List<String> nodes = Arrays.asList("node-A", "node-B");
        RaftNode node = new RaftNode("node-A", nodes, testConfig, mockPersistence, mockMetrics, testPort);

        try {
            VoteRequest request = new VoteRequest(Long.MAX_VALUE, "node-B");
            VoteResponse response = node.handleVoteRequest(request);
            assertNotNull(response);
            assertEquals(Long.MAX_VALUE, response.term);
        } finally {
            node.shutdown();
        }
    }
}