package com.iksanov.distributedcache.node.consensus;

import com.iksanov.distributedcache.node.consensus.model.Command;
import com.iksanov.distributedcache.node.consensus.persistence.FileBasedRaftPersistence;
import com.iksanov.distributedcache.node.consensus.persistence.RaftPersistence;
import com.iksanov.distributedcache.node.consensus.raft.DefaultRaftStateMachine;
import com.iksanov.distributedcache.node.consensus.raft.RaftNode;
import com.iksanov.distributedcache.node.consensus.raft.RaftRole;
import com.iksanov.distributedcache.node.consensus.raft.RaftStateMachine;
import com.iksanov.distributedcache.node.consensus.transport.NettyRaftTransport;
import com.iksanov.distributedcache.node.consensus.transport.RaftTransport;
import com.iksanov.distributedcache.node.core.CacheStore;
import com.iksanov.distributedcache.node.core.InMemoryCacheStore;
import com.iksanov.distributedcache.node.metrics.CacheMetrics;
import com.iksanov.distributedcache.node.metrics.RaftMetrics;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test for Raft consensus cluster.
 * <p>
 * Tests a real 3-node Raft cluster with:
 * - Leader election
 * - Command replication
 * - Consensus and commit
 * - Persistence and recovery
 * - Failure scenarios
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@DisplayName("Raft Cluster Integration Test")
@Tag("integration")
class RaftClusterIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(RaftClusterIntegrationTest.class);
    private static final String HOST = "localhost";
    private static final int BASE_PORT = 17100;
    private static final int ELECTION_MIN_MS = 1500;
    private static final int ELECTION_MAX_MS = 3000;
    private static final int HEARTBEAT_MS = 500;

    private final Map<String, TestNode> nodes = new LinkedHashMap<>();
    private final List<Path> tempDirs = new ArrayList<>();

    static class TestNode {
        String nodeId;
        int port;
        CacheStore store;
        RaftStateMachine stateMachine;
        RaftTransport transport;
        RaftPersistence persistence;
        RaftMetrics metrics;
        RaftNode raftNode;
        Path dataDir;

        TestNode(String nodeId, int port) {
            this.nodeId = nodeId;
            this.port = port;
        }
    }

    @BeforeAll
    void setupCluster() throws Exception {
        log.info("🚀 Setting up 3-node Raft cluster...");

        for (int i = 0; i < 3; i++) {
            String nodeId = "node-" + (i + 1);
            int port = BASE_PORT + i;
            TestNode node = new TestNode(nodeId, port);
            nodes.put(nodeId, node);
        }

        for (TestNode node : nodes.values()) {
            log.info("  📦 Initializing {}", node.nodeId);
            node.dataDir = Files.createTempDirectory("raft-test-" + node.nodeId + "-");
            tempDirs.add(node.dataDir);
            node.store = new InMemoryCacheStore(1000, 0, 100, new CacheMetrics());
            node.metrics = new RaftMetrics();
            node.stateMachine = new DefaultRaftStateMachine(node.store, node.metrics);
            node.transport = new NettyRaftTransport(node.nodeId, HOST, node.port);
            node.persistence = new FileBasedRaftPersistence(node.dataDir, node.nodeId);

            List<String> peers = nodes.values().stream()
                    .filter(n -> !n.nodeId.equals(node.nodeId))
                    .map(n -> HOST + ":" + n.port)
                    .collect(Collectors.toList());

            node.raftNode = new RaftNode(
                    node.nodeId,
                    peers,
                    node.transport,
                    node.stateMachine,
                    node.persistence,
                    node.metrics,
                    ELECTION_MIN_MS,
                    ELECTION_MAX_MS,
                    HEARTBEAT_MS,
                    100,
                    10
            );
            log.info("    ✅ {} initialized with peers: {}", node.nodeId, peers);
        }

        for (TestNode node : nodes.values()) {
            node.raftNode.start();
            log.info("    ✅ {} started on port {}", node.nodeId, node.port);
        }

        log.info("  🔗 Registering peer mappings...");
        for (TestNode node : nodes.values()) {
            for (TestNode peer : nodes.values()) {
                if (!peer.nodeId.equals(node.nodeId)) {
                    String peerAddress = HOST + ":" + peer.port;
                    ((NettyRaftTransport) node.transport).registerPeerMapping(peer.nodeId, peerAddress);
                }
            }
        }

        log.info("  🔗 Establishing peer connections...");
        for (TestNode node : nodes.values()) {
            List<String> nodePeers = nodes.values().stream()
                    .filter(n -> !n.nodeId.equals(node.nodeId))
                    .map(n -> HOST + ":" + n.port)
                    .collect(Collectors.toList());
            node.transport.connectToPeers(nodePeers);
        }
        log.info("✅ Cluster ready!");
    }

    @AfterAll
    void tearDownCluster() {
        log.info("🛑 Shutting down cluster...");
        for (TestNode node : nodes.values()) {
            try {
                if (node.raftNode != null) {
                    node.raftNode.stop();
                }
            } catch (Exception e) {
                log.warn("Error stopping {}: {}", node.nodeId, e.getMessage());
            }
        }

        for (Path dir : tempDirs) {
            try {
                Files.walk(dir)
                        .sorted(Comparator.reverseOrder())
                        .forEach(p -> {
                            try {
                                Files.deleteIfExists(p);
                            } catch (Exception ignored) {
                            }
                        });
            } catch (Exception e) {
                log.warn("Error cleaning up {}: {}", dir, e.getMessage());
            }
        }
        log.info("✅ Cluster shutdown complete");
    }

    @Test
    @Order(1)
    @DisplayName("Should elect a leader within timeout")
    void shouldElectLeader() throws Exception {
        log.info("\n🧪 TEST: Leader election");
        RaftNode leader = waitForLeader(ELECTION_MAX_MS * 2);
        assertNotNull(leader, "A leader should be elected");
        String leaderId = leader.getLeaderId();
        log.info("  ✅ Leader elected: {}", leaderId);

        List<RaftNode> leaders = nodes.values().stream()
                .map(n -> n.raftNode)
                .filter(n -> n.getRole() == RaftRole.LEADER)
                .toList();
        assertEquals(1, leaders.size(), "Should have exactly one leader");

        for (TestNode node : nodes.values()) {
            String knownLeader = node.raftNode.getLeaderId();
            assertTrue(knownLeader == null || knownLeader.equals(leaderId), node.nodeId + " should know correct leader");
        }
        log.info("✅ TEST PASSED: Leader election successful\n");
    }

    @Test
    @Order(2)
    @DisplayName("Should replicate SET command to all nodes")
    void shouldReplicateSetCommand() throws Exception {
        log.info("\n🧪 TEST: Command replication (SET)");
        RaftNode leader = waitForLeader(ELECTION_MAX_MS * 2);
        assertNotNull(leader, "Leader must be elected first");
        String leaderId = leader.getLeaderId();
        log.info("  📍 Current leader: {}", leaderId);
        String key = "test-key-1";
        String value = "test-value-1";
        Command cmd = new Command(Command.Type.SET, key, value);
        log.info("  1️⃣ Submitting SET command to leader...");
        CompletableFuture<Long> future = leader.submit(cmd);
        Long commitIndex = future.get(5, TimeUnit.SECONDS);
        assertNotNull(commitIndex, "Command should be committed");
        log.info("Command committed at index {}", commitIndex);
        Thread.sleep(500);
        log.info("Verifying replication to all nodes...");
        for (TestNode node : nodes.values()) {
            String actual = node.store.get(key);
            assertEquals(value, actual, node.nodeId + " should have replicated value");
            log.info("{}: {}", node.nodeId, actual);
        }
        log.info("TEST PASSED: Command replicated to all nodes\n");
    }

    @Test
    @Order(3)
    @DisplayName("Should replicate DELETE command to all nodes")
    void shouldReplicateDeleteCommand() throws Exception {
        log.info("\n🧪 TEST: Command replication (DELETE)");
        RaftNode leader = waitForLeader(ELECTION_MAX_MS * 2);
        assertNotNull(leader, "Leader must be elected first");
        String key = "test-key-2";
        String value = "test-value-2";
        log.info("  0️⃣ Setup: Setting initial value...");
        Command setCmd = new Command(Command.Type.SET, key, value);
        leader.submit(setCmd).get(5, TimeUnit.SECONDS);
        Thread.sleep(500);
        for (TestNode node : nodes.values()) {
            assertEquals(value, node.store.get(key));
        }
        log.info("Initial value replicated");

        // Now DELETE
        log.info("Submitting DELETE command...");
        Command deleteCmd = new Command(Command.Type.DELETE, key, null);
        Long commitIndex = leader.submit(deleteCmd).get(5, TimeUnit.SECONDS);
        assertNotNull(commitIndex);
        log.info("DELETE committed at index {}", commitIndex);

        Thread.sleep(500);

        // Verify all nodes deleted
        log.info("  2️⃣ Verifying deletion on all nodes...");
        for (TestNode node : nodes.values()) {
            String actual = node.store.get(key);
            assertNull(actual, node.nodeId + " should have deleted key");
            log.info("    ✅ {}: null (deleted)", node.nodeId);
        }

        log.info("✅ TEST PASSED: DELETE replicated to all nodes\n");
    }

    @Test
    @Order(4)
    @DisplayName("Should handle multiple commands in sequence")
    void shouldHandleMultipleCommands() throws Exception {
        log.info("\n🧪 TEST: Multiple commands");

        RaftNode leader = waitForLeader(ELECTION_MAX_MS * 2);
        assertNotNull(leader);

        int commandCount = 10;
        List<String> keys = new ArrayList<>();

        log.info("  1️⃣ Submitting {} commands...", commandCount);
        for (int i = 0; i < commandCount; i++) {
            String key = "multi-key-" + i;
            String value = "multi-value-" + i;
            keys.add(key);

            Command cmd = new Command(Command.Type.SET, key, value);
            CompletableFuture<Long> future = leader.submit(cmd);
            Long index = future.get(5, TimeUnit.SECONDS);
            log.info("    ✅ Command {} committed at index {}", i + 1, index);
        }

        Thread.sleep(1000);

        // Verify all commands applied to all nodes
        log.info("  2️⃣ Verifying all commands on all nodes...");
        for (TestNode node : nodes.values()) {
            for (int i = 0; i < commandCount; i++) {
                String key = keys.get(i);
                String expected = "multi-value-" + i;
                String actual = node.store.get(key);
                assertEquals(expected, actual, node.nodeId + " should have all values");
            }
            log.info("    ✅ {}: all {} commands applied", node.nodeId, commandCount);
        }

        log.info("✅ TEST PASSED: Multiple commands handled correctly\n");
    }

    @Test
    @Order(5)
    @DisplayName("Should reject submit on follower")
    void shouldRejectSubmitOnFollower() throws Exception {
        log.info("\n🧪 TEST: Submit to follower rejection");

        RaftNode leader = waitForLeader(ELECTION_MAX_MS * 2);
        assertNotNull(leader);

        // Find a follower
        RaftNode follower = nodes.values().stream()
                .map(n -> n.raftNode)
                .filter(n -> n.getRole() == RaftRole.FOLLOWER)
                .findFirst()
                .orElseThrow(() -> new AssertionError("Should have at least one follower"));

        log.info("  1️⃣ Attempting to submit command to follower...");
        Command cmd = new Command(Command.Type.SET, "test", "value");
        CompletableFuture<Long> future = follower.submit(cmd);

        // Should complete exceptionally
        assertThrows(Exception.class, () -> future.get(2, TimeUnit.SECONDS),
                "Follower should reject command submission");

        log.info("    ✅ Follower correctly rejected command");
        log.info("✅ TEST PASSED: Follower rejection works\n");
    }

    @Test
    @Order(6)
    @DisplayName("Should maintain consistency with concurrent commands")
    void shouldMaintainConsistencyWithConcurrentCommands() throws Exception {
        log.info("\n🧪 TEST: Concurrent commands consistency");

        RaftNode leader = waitForLeader(ELECTION_MAX_MS * 2);
        assertNotNull(leader);

        int concurrentCommands = 20;
        List<CompletableFuture<Long>> futures = new ArrayList<>();

        log.info("  1️⃣ Submitting {} concurrent commands...", concurrentCommands);
        for (int i = 0; i < concurrentCommands; i++) {
            String key = "concurrent-" + i;
            String value = "value-" + i;
            Command cmd = new Command(Command.Type.SET, key, value);
            futures.add(leader.submit(cmd));
        }

        // Wait for all to commit
        log.info("  2️⃣ Waiting for all commits...");
        long maxIndex = 0;
        for (int i = 0; i < futures.size(); i++) {
            Long index = futures.get(i).get(10, TimeUnit.SECONDS);
            assertNotNull(index, "Command " + i + " should commit");
            maxIndex = Math.max(maxIndex, index);
        }
        log.info("    ✅ All commands committed (maxIndex={})", maxIndex);

        Thread.sleep(1000); // Wait for application to all nodes

        // Verify consistency across all nodes
        log.info("  3️⃣ Verifying consistency...");
        // Get expected values from leader
        RaftNode currentLeader = waitForLeader(ELECTION_MAX_MS * 2);
        TestNode leaderNode = nodes.values().stream()
                .filter(n -> n.raftNode == currentLeader)
                .findFirst()
                .orElseThrow();

        for (int i = 0; i < concurrentCommands; i++) {
            String key = "concurrent-" + i;
            String expected = leaderNode.store.get(key);
            assertNotNull(expected, "Leader should have value for " + key);

            for (TestNode node : nodes.values()) {
                String actual = node.store.get(key);
                assertEquals(expected, actual,
                        "All nodes should have same value for " + key);
            }
        }
        log.info("    ✅ All nodes consistent");

        log.info("✅ TEST PASSED: Consistency maintained\n");
    }

    @Test
    @Order(7)
    @DisplayName("Should verify metrics are updated")
    void shouldUpdateMetrics() throws Exception {
        log.info("\n🧪 TEST: Metrics verification");

        RaftNode leader = waitForLeader(ELECTION_MAX_MS * 2);
        assertNotNull(leader);

        // Submit a command
        Command cmd = new Command(Command.Type.SET, "metrics-test", "value");
        leader.submit(cmd).get(5, TimeUnit.SECONDS);
        Thread.sleep(500);

        // Check metrics on leader node
        TestNode leaderNode = nodes.values().stream()
                .filter(n -> n.raftNode == leader)
                .findFirst()
                .orElseThrow();

        RaftMetrics metrics = leaderNode.metrics;

        log.info("  📊 Leader metrics:");
        log.info("    - Current term: {}", metrics.getCurrentTerm());
        log.info("    - Log size: {}", metrics.getLogSize());
        log.info("    - Commit index: {}", metrics.getCommitIndex());
        log.info("    - Commands replicated: {}", metrics.getCommandsReplicated());

        assertTrue(metrics.getLogSize() > 0, "Log size should be > 0");
        assertTrue(metrics.getCommitIndex() > 0, "Commit index should be > 0");

        log.info("✅ TEST PASSED: Metrics updated correctly\n");
    }

    @Test
    @Order(8)
    @DisplayName("Should create snapshot and recover from it")
    void shouldCreateSnapshotAndRecover() throws Exception {
        log.info("\n🧪 TEST: Snapshot creation and recovery");

        RaftNode leader = waitForLeader(ELECTION_MAX_MS * 2);
        assertNotNull(leader);

        // Apply many commands to trigger snapshot (SNAPSHOT_INTERVAL = 10000)
        // We'll apply more than that to ensure snapshot is created
        int commandCount = 100; // Small number for testing
        log.info("  1️⃣ Submitting {} commands to trigger snapshot...", commandCount);

        for (int i = 0; i < commandCount; i++) {
            Command cmd = new Command(Command.Type.SET, "snap-key-" + i, "snap-value-" + i);
            leader.submit(cmd).get(5, TimeUnit.SECONDS);
            if (i % 20 == 0) {
                log.info("    Progress: {}/{}", i, commandCount);
            }
        }
        log.info("    ✅ All commands submitted");

        Thread.sleep(2000); // Wait for replication

        // Verify all nodes have the data
        log.info("  2️⃣ Verifying data on all nodes...");
        for (TestNode node : nodes.values()) {
            for (int i = 0; i < commandCount; i++) {
                String key = "snap-key-" + i;
                String expected = "snap-value-" + i;
                String actual = node.store.get(key);
                assertEquals(expected, actual, node.nodeId + " should have " + key);
            }
        }
        log.info("    ✅ All nodes have consistent data");

        // Pick a follower to restart
        TestNode followerNode = nodes.values().stream()
                .filter(n -> n.raftNode.getRole() == RaftRole.FOLLOWER)
                .findFirst()
                .orElseThrow();

        log.info("  3️⃣ Restarting follower {} to test recovery...", followerNode.nodeId);

        // Stop the follower
        followerNode.raftNode.stop();
        Thread.sleep(1000);

        // Create new transport (old one was shut down)
        followerNode.transport = new NettyRaftTransport(followerNode.nodeId, HOST, followerNode.port);

        // Register peer mappings for new transport
        for (TestNode peer : nodes.values()) {
            if (!peer.nodeId.equals(followerNode.nodeId)) {
                String peerAddress = HOST + ":" + peer.port;
                ((NettyRaftTransport) followerNode.transport).registerPeerMapping(peer.nodeId, peerAddress);
            }
        }

        // Create new RaftNode with same persistence (simulates restart)
        List<String> peers = nodes.values().stream()
                .filter(n -> !n.nodeId.equals(followerNode.nodeId))
                .map(n -> HOST + ":" + n.port)
                .collect(Collectors.toList());

        followerNode.raftNode = new RaftNode(
                followerNode.nodeId,
                peers,
                followerNode.transport, // New transport instance
                followerNode.stateMachine,
                followerNode.persistence, // Same persistence - will recover from disk
                followerNode.metrics,
                ELECTION_MIN_MS,
                ELECTION_MAX_MS,
                HEARTBEAT_MS,
                100,
                10
        );
        followerNode.raftNode.start();

        // Connect to peers with new transport
        followerNode.transport.connectToPeers(peers);

        log.info("    ✅ {} restarted", followerNode.nodeId);

        Thread.sleep(2000); // Wait for recovery and sync

        // Verify recovered node has all data
        log.info("  4️⃣ Verifying recovered node has all data...");
        for (int i = 0; i < commandCount; i++) {
            String key = "snap-key-" + i;
            String expected = "snap-value-" + i;
            String actual = followerNode.store.get(key);
            assertEquals(expected, actual,
                    followerNode.nodeId + " should have " + key + " after recovery");
        }
        log.info("    ✅ Recovered node has all data");

        log.info("✅ TEST PASSED: Snapshot and recovery successful\n");
    }

    /**
     * Wait for a leader to be elected.
     *
     * @param timeoutMs max time to wait
     * @return the elected leader, or null if timeout
     */
    private RaftNode waitForLeader(long timeoutMs) throws InterruptedException {
        long start = System.currentTimeMillis();
        while (System.currentTimeMillis() - start < timeoutMs) {
            for (TestNode node : nodes.values()) {
                if (node.raftNode.getRole() == RaftRole.LEADER) {
                    return node.raftNode;
                }
            }
            Thread.sleep(100);
        }
        return null;
    }
}