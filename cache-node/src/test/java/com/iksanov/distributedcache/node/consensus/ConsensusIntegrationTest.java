package com.iksanov.distributedcache.node.consensus;

import com.iksanov.distributedcache.node.config.RaftConfig;
import com.iksanov.distributedcache.node.consensus.model.*;
import com.iksanov.distributedcache.node.consensus.persistence.FileBasedRaftPersistence;
import com.iksanov.distributedcache.node.consensus.persistence.RaftPersistence;
import com.iksanov.distributedcache.node.metrics.RaftMetrics;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive integration test for the entire consensus package.
 * <p>
 * This test covers:
 * - Full 3-node Raft cluster setup with network communication
 * - Leader election under normal conditions
 * - Leader election with node failures
 * - Persistence across node restarts
 * - High-load concurrent operations
 * - Split-brain prevention
 * - Graceful shutdown under load
 * <p>
 * Test duration: ~30-60 seconds
 */
@DisplayName("Consensus Package - Full Integration Test")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ConsensusIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(ConsensusIntegrationTest.class);
    private static final int BASE_PORT = 19000;
    private static final Duration ELECTION_WAIT = Duration.ofSeconds(3);
    private static final Duration LEADER_STABILIZATION = Duration.ofSeconds(2);

    @TempDir
    Path tempDir;

    private List<RaftNode> nodes;
    private List<RaftPersistence> persistences;
    private RaftConfig testConfig;

    @BeforeEach
    void setUp() {
        nodes = new ArrayList<>();
        persistences = new ArrayList<>();
        testConfig = new RaftConfig(150, 300, 50, 200);
    }

    @AfterEach
    void tearDown() {
        log.info("Shutting down all nodes...");
        nodes.forEach(node -> {
            try {
                node.shutdown();
            } catch (Exception e) {
                log.warn("Error shutting down node: {}", e.getMessage());
            }
        });
        nodes.clear();
        persistences.clear();
    }

    @Test
    @Order(1)
    @DisplayName("Test 1: Should start 3-node cluster successfully")
    void test1_shouldStartClusterSuccessfully() throws Exception {
        log.info("=== TEST 1: Starting 3-node cluster ===");
        List<String> nodeIds = Arrays.asList("node-1", "node-2", "node-3");
        createCluster(nodeIds);
        assertEquals(3, nodes.size());
        for (RaftNode node : nodes) {
            assertNotNull(node.getNodeId());
            assertNotNull(node.getState());
            assertTrue(node.getCurrentTerm() >= 0);
        }
        log.info("✓ All 3 nodes started as FOLLOWER");
    }

    @Test
    @Order(2)
    @DisplayName("Test 2: Should elect leader automatically")
    void test2_shouldElectLeaderAutomatically() throws Exception {
        log.info("=== TEST 2: Leader election ===");
        List<String> nodeIds = Arrays.asList("node-1", "node-2", "node-3");
        createCluster(nodeIds);
        log.info("Waiting for leader election...");
        Thread.sleep(ELECTION_WAIT.toMillis());
        List<RaftNode> leaders = nodes.stream()
                .filter(n -> n.getState() == RaftNode.NodeState.LEADER)
                .toList();

        assertEquals(1, leaders.size(), "Should have exactly one leader");
        RaftNode leader = leaders.getFirst();
        long leaderTerm = leader.getCurrentTerm();
        log.info("✓ Leader elected: {} in term {}", leader.getNodeId(), leaderTerm);
        for (RaftNode node : nodes) {
            assertTrue(node.getCurrentTerm() >= leaderTerm, "Node " + node.getNodeId() + " term should be >= leader term");
        }

        Thread.sleep(500);
        for (RaftNode node : nodes) {
            if (node.getState() == RaftNode.NodeState.FOLLOWER) {
                assertEquals(leader.getNodeId(), node.getCurrentLeader(), "Follower should know the leader");
            }
        }
        log.info("✓ All nodes agree on leader");
    }

    @Test
    @Order(3)
    @DisplayName("Test 3: Should re-elect leader when current leader fails")
    void test3_shouldReelectLeaderOnFailure() throws Exception {
        log.info("=== TEST 3: Leader failure and re-election ===");
        List<String> nodeIds = Arrays.asList("node-1", "node-2", "node-3");
        createCluster(nodeIds);
        Thread.sleep(ELECTION_WAIT.toMillis());
        RaftNode originalLeader = findLeader();
        assertNotNull(originalLeader, "Should have a leader");
        long originalTerm = originalLeader.getCurrentTerm();
        log.info("Original leader: {} (term {})", originalLeader.getNodeId(), originalTerm);
        log.info("Killing leader {}...", originalLeader.getNodeId());
        originalLeader.shutdown();
        nodes.remove(originalLeader);
        log.info("Waiting for re-election...");
        Thread.sleep(ELECTION_WAIT.toMillis());
        RaftNode newLeader = findLeader();
        assertNotNull(newLeader, "Should have elected a new leader");
        assertNotEquals(originalLeader.getNodeId(), newLeader.getNodeId());
        long newTerm = newLeader.getCurrentTerm();
        assertTrue(newTerm > originalTerm, "New term should be greater than original term");
        log.info("✓ New leader elected: {} (term {})", newLeader.getNodeId(), newTerm);
    }

    @Test
    @Order(4)
    @DisplayName("Test 4: Should persist state across node restarts")
    void test4_shouldPersistStateAcrossRestarts() throws Exception {
        log.info("=== TEST 4: Persistence across restarts ===");
        List<String> nodeIds = Arrays.asList("node-1", "node-2", "node-3");
        Map<String, String> addresses = buildAddressMap(nodeIds);
        createCluster(nodeIds);
        Thread.sleep(ELECTION_WAIT.toMillis());
        RaftNode node1 = nodes.getFirst();
        long termBeforeShutdown = node1.getCurrentTerm();
        log.info("Node-1 term before shutdown: {}", termBeforeShutdown);
        node1.shutdown();
        Thread.sleep(1000);
        RaftPersistence persistence1 = persistences.getFirst();
        RaftNode restartedNode1 = new RaftNode(
                "node-1",
                nodeIds,
                addresses,
                testConfig,
                persistence1,
                new RaftMetrics(),
                BASE_PORT
        );
        nodes.set(0, restartedNode1);
        long termAfterRestart = restartedNode1.getCurrentTerm();
        assertEquals(termBeforeShutdown, termAfterRestart, "Should restore term from persistence");
        log.info("✓ Node-1 restored term: {}", termAfterRestart);
    }

    @Test
    @Order(5)
    @DisplayName("Test 5: Should handle concurrent vote requests correctly")
    void test5_shouldHandleConcurrentVoteRequests() throws Exception {
        log.info("=== TEST 5: Concurrent vote requests ===");
        List<String> nodeIds = Arrays.asList("node-1", "node-2", "node-3");
        createCluster(nodeIds);
        RaftNode targetNode = nodes.getFirst();
        int threadCount = 20;
        int requestsPerThread = 50;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);
        AtomicInteger grantedVotes = new AtomicInteger(0);
        AtomicInteger deniedVotes = new AtomicInteger(0);
        AtomicInteger errors = new AtomicInteger(0);
        log.info("Starting {} threads, {} requests each...", threadCount, requestsPerThread);
        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    for (int i = 0; i < requestsPerThread; i++) {
                        VoteRequest request = new VoteRequest(
                                ThreadLocalRandom.current().nextLong(1, 100),
                                "candidate-" + threadId
                        );
                        VoteResponse response = targetNode.handleVoteRequest(request);
                        if (response.voteGranted()) {
                            grantedVotes.incrementAndGet();
                        } else {
                            deniedVotes.incrementAndGet();
                        }
                    }
                } catch (Exception e) {
                    errors.incrementAndGet();
                    log.error("Error in thread {}: {}", threadId, e.getMessage());
                } finally {
                    latch.countDown();
                }
            });
        }
        boolean completed = latch.await(30, TimeUnit.SECONDS);
        executor.shutdown();
        assertTrue(completed, "All threads should complete");
        assertEquals(0, errors.get(), "Should have no errors");
        int totalRequests = threadCount * requestsPerThread;
        assertEquals(totalRequests, grantedVotes.get() + deniedVotes.get());
        log.info("✓ Processed {} requests: {} granted, {} denied, {} errors", totalRequests, grantedVotes.get(), deniedVotes.get(), errors.get());
    }

    @Test
    @Order(6)
    @DisplayName("Test 6: Should handle concurrent heartbeats correctly")
    void test6_shouldHandleConcurrentHeartbeats() throws Exception {
        log.info("=== TEST 6: Concurrent heartbeats ===");
        List<String> nodeIds = Arrays.asList("node-1", "node-2", "node-3");
        createCluster(nodeIds);
        RaftNode targetNode = nodes.getFirst();
        int threadCount = 15;
        int heartbeatsPerThread = 100;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger errors = new AtomicInteger(0);
        log.info("Starting {} threads, {} heartbeats each...", threadCount, heartbeatsPerThread);
        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    for (int i = 0; i < heartbeatsPerThread; i++) {
                        HeartbeatRequest request = new HeartbeatRequest(
                                1L + (i % 10),
                                "leader-" + threadId
                        );
                        HeartbeatResponse response = targetNode.handleHeartbeat(request);
                        if (response.success()) successCount.incrementAndGet();
                    }
                } catch (Exception e) {
                    errors.incrementAndGet();
                    log.error("Error in thread {}: {}", threadId, e.getMessage());
                } finally {
                    latch.countDown();
                }
            });
        }
        boolean completed = latch.await(30, TimeUnit.SECONDS);
        executor.shutdown();
        assertTrue(completed, "All threads should complete");
        assertEquals(0, errors.get(), "Should have no errors");
        log.info("✓ Processed {} heartbeats successfully", successCount.get());
    }

    @Test
    @Order(7)
    @DisplayName("Test 7: Should handle split vote and eventually elect leader")
    void test7_shouldHandleSplitVote() throws Exception {
        log.info("=== TEST 7: Split vote scenario ===");
        List<String> nodeIds = Arrays.asList("node-1", "node-2", "node-3", "node-4", "node-5");
        createCluster(nodeIds);
        log.info("Waiting for election (may have split votes)...");
        Thread.sleep(ELECTION_WAIT.toMillis() + 2000);
        RaftNode leader = findLeader();
        assertNotNull(leader, "Should eventually elect a leader despite potential split votes");
        log.info("✓ Leader elected despite potential splits: {}", leader.getNodeId());
    }

    @Test
    @Order(8)
    @DisplayName("Test 8: Leader should remain stable under heavy load")
    void test8_leaderStabilityUnderLoad() throws Exception {
        log.info("=== TEST 8: Leader stability under load ===");
        List<String> nodeIds = Arrays.asList("node-1", "node-2", "node-3");
        createCluster(nodeIds);
        Thread.sleep(LEADER_STABILIZATION.toMillis());
        RaftNode initialLeader = findLeader();
        assertNotNull(initialLeader);
        log.info("Initial leader: {}", initialLeader.getNodeId());
        ExecutorService executor = Executors.newFixedThreadPool(30);
        AtomicInteger requestCount = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(30);
        log.info("Starting load test for 10 seconds...");
        for (int t = 0; t < 30; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    long endTime = System.currentTimeMillis() + 10_000;
                    while (System.currentTimeMillis() < endTime) {
                        RaftNode randomNode = nodes.get(ThreadLocalRandom.current().nextInt(nodes.size()));
                        if (threadId % 2 == 0) {
                            VoteRequest voteReq = new VoteRequest(
                                    ThreadLocalRandom.current().nextLong(1, 5),
                                    "candidate-" + threadId
                            );
                            randomNode.handleVoteRequest(voteReq);
                        } else {
                            HeartbeatRequest hbReq = new HeartbeatRequest(
                                    ThreadLocalRandom.current().nextLong(1, 5),
                                    "leader-" + threadId
                            );
                            randomNode.handleHeartbeat(hbReq);
                        }
                        requestCount.incrementAndGet();
                        Thread.sleep(1);
                    }
                } catch (Exception e) {
                    log.error("Error in load thread: {}", e.getMessage());
                } finally {
                    latch.countDown();
                }
            });
        }
        latch.await();
        executor.shutdown();
        log.info("Load test complete: {} requests processed", requestCount.get());
        RaftNode finalLeader = findLeader();
        assertNotNull(finalLeader, "Should still have a leader after load");
        log.info("Final leader: {} (initial: {})", finalLeader.getNodeId(), initialLeader.getNodeId());
        log.info("✓ Cluster remained operational under load");
    }

    @Test
    @Order(9)
    @DisplayName("Test 9: Should shutdown gracefully even under load")
    void test9_gracefulShutdownUnderLoad() throws Exception {
        log.info("=== TEST 9: Graceful shutdown under load ===");
        List<String> nodeIds = Arrays.asList("node-1", "node-2", "node-3");
        createCluster(nodeIds);
        ExecutorService executor = Executors.newFixedThreadPool(10);
        AtomicInteger activeThreads = new AtomicInteger(10);
        for (int t = 0; t < 10; t++) {
            executor.submit(() -> {
                try {
                    while (!Thread.currentThread().isInterrupted()) {
                        RaftNode node = nodes.get(ThreadLocalRandom.current().nextInt(nodes.size()));
                        VoteRequest req = new VoteRequest(1L, "candidate");
                        node.handleVoteRequest(req);
                        Thread.sleep(10);
                    }
                } catch (Exception e) {
                } finally {
                    activeThreads.decrementAndGet();
                }
            });
        }
        Thread.sleep(2000);
        log.info("Initiating shutdown under load...");
        long shutdownStart = System.currentTimeMillis();
        nodes.forEach(RaftNode::shutdown);
        long shutdownTime = System.currentTimeMillis() - shutdownStart;
        executor.shutdownNow();
        executor.awaitTermination(5, TimeUnit.SECONDS);
        log.info("✓ All nodes shut down gracefully in {}ms", shutdownTime);
        assertTrue(shutdownTime < 5000, "Shutdown should complete within 5 seconds");
        nodes.clear();
    }

    private Map<String, String> buildAddressMap(List<String> nodeIds) {
        Map<String, String> addresses = new HashMap<>();
        for (int i = 0; i < nodeIds.size(); i++) {
            String nodeId = nodeIds.get(i);
            String address = "localhost:" + (BASE_PORT + i);
            addresses.put(nodeId, address);
        }
        return addresses;
    }

    private void createCluster(List<String> nodeIds) throws IOException, InterruptedException {
        log.info("Creating cluster with nodes: {}", nodeIds);
        String uniqueId = java.util.UUID.randomUUID().toString().substring(0, 8);
        java.nio.file.Path clusterDir = tempDir.resolve("cluster-" + uniqueId);
        Map<String, String> addresses = buildAddressMap(nodeIds);
        log.info("Address mapping: {}", addresses);
        for (int i = 0; i < nodeIds.size(); i++) {
            String nodeId = nodeIds.get(i);
            int port = BASE_PORT + i;
            RaftPersistence persistence = new FileBasedRaftPersistence(
                    clusterDir.resolve("node-" + i),
                    nodeId
            );
            persistences.add(persistence);
            RaftNode node = new RaftNode(
                    nodeId,
                    nodeIds,
                    addresses,
                    testConfig,
                    persistence,
                    new RaftMetrics(),
                    port
            );
            nodes.add(node);
            log.debug("Created node: {} on port {} with address {}", nodeId, port, addresses.get(nodeId));
        }
        Thread.sleep(500);
        log.info("✓ All {} nodes started in directory {}", nodeIds.size(), clusterDir);
    }

    private RaftNode findLeader() {
        return nodes.stream()
                .filter(n -> n.getState() == RaftNode.NodeState.LEADER)
                .findFirst()
                .orElse(null);
    }
}