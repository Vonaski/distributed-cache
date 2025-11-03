package com.iksanov.distributedcache.node.replication.stress;

import com.iksanov.distributedcache.common.cluster.NodeInfo;
import com.iksanov.distributedcache.common.cluster.ReplicaManager;
import com.iksanov.distributedcache.node.core.CacheStore;
import com.iksanov.distributedcache.node.core.InMemoryCacheStore;
import com.iksanov.distributedcache.node.metrics.CacheMetrics;
import com.iksanov.distributedcache.node.replication.*;
import io.netty.channel.nio.NioEventLoopGroup;
import org.junit.jupiter.api.*;

import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.*;

/**
 * End-to-end load test for replication system.
 * <p>
 * Tests realistic workload scenarios with actual network communication
 * between master and replica nodes.
 * <p>
 * Note: This test uses real network I/O and may take several seconds to complete.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ReplicationLoadTest {

    private static final int OPERATION_COUNT = 1000;
    private static final long WAIT_TIMEOUT_MS = 10_000;

    private NioEventLoopGroup sharedEventLoopGroup;
    private ReplicationManager masterReplicationManager;
    private ReplicationSender sender;
    private CacheStore masterStore;
    private CacheStore replicaStore;
    private NodeInfo masterNode;
    private NodeInfo replicaNode;
    private ReplicaManager replicaManager;
    private ReplicationReceiver replicaReceiver;
    private ReplicationReceiver masterReceiver;
    private CacheMetrics metrics;

    @BeforeAll
    void setup() throws Exception {
        sharedEventLoopGroup = new NioEventLoopGroup(4);
        metrics = new CacheMetrics();

        masterNode = new NodeInfo("master-load", "127.0.0.1", 9800);
        replicaNode = new NodeInfo("replica-load", "127.0.0.1", 9801);

        masterStore = new InMemoryCacheStore(100_000, 0, 5_000, metrics);
        replicaStore = new InMemoryCacheStore(100_000, 0, 5_000, metrics);

        replicaReceiver = new ReplicationReceiver(
                replicaNode.host(),
                replicaNode.replicationPort(),
                replicaStore
        );
        replicaReceiver.start();

        masterReceiver = new ReplicationReceiver(
                masterNode.host(),
                masterNode.replicationPort(),
                masterStore
        );
        masterReceiver.start();

        Thread.sleep(500);

        replicaManager = new ReplicaManager();
        replicaManager.registerReplica(masterNode, replicaNode);

        sender = new ReplicationSender(replicaManager, sharedEventLoopGroup);

        Function<String, NodeInfo> primaryResolver = key -> masterNode;
        masterReplicationManager = new ReplicationManager(
                masterNode,
                sender,
                masterReceiver,
                primaryResolver
        );

        System.out.println("Testing connectivity...");
        masterReplicationManager.onLocalSet("test-key", "test-value");
        Thread.sleep(1000);

        String testValue = replicaStore.get("test-key");
        if (testValue == null) {
            throw new IllegalStateException(
                    "Connectivity test failed - replica did not receive test message"
            );
        }
        System.out.println("Connectivity test passed: " + testValue);
    }

    @AfterAll
    void tearDown() {
        if (masterReplicationManager != null) {
            masterReplicationManager.shutdown();
        }

        if (replicaReceiver != null) {
            try {
                replicaReceiver.stop();
            } catch (Exception e) {
                System.err.println("Error stopping replica receiver: " + e);
            }
        }

        if (masterReceiver != null) {
            try {
                masterReceiver.stop();
            } catch (Exception e) {
                System.err.println("Error stopping master receiver: " + e);
            }
        }

        if (sharedEventLoopGroup != null) {
            sharedEventLoopGroup.shutdownGracefully();
        }

        System.out.println("Shutdown complete");
    }

    @Test
    @DisplayName("Should replicate operations under sustained load")
    void shouldReplicateUnderSustainedLoad() throws Exception {
        System.out.printf("Starting load test with %d operations%n", OPERATION_COUNT);
        long startTime = System.nanoTime();

        for (int i = 0; i < OPERATION_COUNT; i++) {
            masterReplicationManager.onLocalSet("key-" + i, "val-" + i);

            if (i > 0 && i % 100 == 0) {
                Thread.sleep(10);
            }
        }

        long sendDurationMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime);
        System.out.printf("All %d operations sent in %d ms%n", OPERATION_COUNT, sendDurationMs);
        boolean success = awaitReplicaProgress(OPERATION_COUNT - 1, WAIT_TIMEOUT_MS);
        assertTrue(success, "Replication must complete within timeout");
        long totalDurationMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime);

        String lastKey = "key-" + (OPERATION_COUNT - 1);
        String lastValue = replicaStore.get(lastKey);
        assertEquals("val-" + (OPERATION_COUNT - 1), lastValue, "Last key must be present in replica");

        int sampleSize = Math.min(100, OPERATION_COUNT);
        int verifiedKeys = 0;
        for (int i = 0; i < sampleSize; i++) {
            int idx = i * (OPERATION_COUNT / sampleSize);
            String key = "key-" + idx;
            String expectedValue = "val-" + idx;
            String actualValue = replicaStore.get(key);
            if (expectedValue.equals(actualValue)) {
                verifiedKeys++;
            }
        }

        System.out.printf("%n=== Load Test Results ===%n");
        System.out.printf("Total operations: %,d%n", OPERATION_COUNT);
        System.out.printf("Send duration: %,d ms%n", sendDurationMs);
        System.out.printf("Total duration: %,d ms%n", totalDurationMs);
        System.out.printf("Throughput: %.2f ops/sec%n", OPERATION_COUNT / (totalDurationMs / 1000.0));
        System.out.printf("Verified keys: %d/%d (%.1f%%)%n", verifiedKeys, sampleSize, 100.0 * verifiedKeys / sampleSize);

        assertTrue(verifiedKeys >= sampleSize * 0.95, "At least 95% of sampled keys must be replicated correctly");
    }

    @Test
    @DisplayName("Should handle mixed SET and DELETE operations")
    void shouldHandleMixedOperations() throws Exception {
        int operationCount = 500;
        int setCount = 0;
        int deleteCount = 0;

        System.out.printf("Starting mixed operations test with %d operations%n", operationCount);

        for (int i = 0; i < operationCount; i++) {
            String key = "mixed-key-" + (i % 100);

            if (i % 3 == 0) {
                masterReplicationManager.onLocalDelete(key);
                deleteCount++;
            } else {
                masterReplicationManager.onLocalSet(key, "value-" + i);
                setCount++;
            }

            if (i > 0 && i % 50 == 0) {
                Thread.sleep(10);
            }
        }

        Thread.sleep(2000);

        System.out.printf("Mixed operations test complete:%n");
        System.out.printf("  SET operations: %d%n", setCount);
        System.out.printf("  DELETE operations: %d%n", deleteCount);
        System.out.printf("  Replica store size: %d%n", replicaStore.size());

        assertTrue(replicaStore.size() > 0, "Replica should contain some keys after mixed operations");
    }

    @Test
    @DisplayName("Should maintain consistency under rapid updates to same keys")
    void shouldMaintainConsistencyWithRapidUpdates() throws Exception {
        int updateCount = 100;
        int keyCount = 10;

        System.out.printf("Testing rapid updates: %d updates across %d keys%n", updateCount, keyCount);

        for (int i = 0; i < updateCount; i++) {
            String key = "rapid-key-" + (i % keyCount);
            String value = "version-" + i;
            masterReplicationManager.onLocalSet(key, value);
        }
        Thread.sleep(2000);
        int presentKeys = 0;
        for (int i = 0; i < keyCount; i++) {
            String key = "rapid-key-" + i;
            if (replicaStore.get(key) != null) {
                presentKeys++;
            }
        }
        System.out.printf("Keys present in replica: %d/%d%n", presentKeys, keyCount);
        assertTrue(presentKeys >= keyCount * 0.9, "At least 90% of keys must be present in replica");
    }

    private boolean awaitReplicaProgress(int lastIndex, long timeoutMs) throws InterruptedException {
        String lastKey = "key-" + lastIndex;
        String expectedValue = "val-" + lastIndex;
        long deadline = System.currentTimeMillis() + timeoutMs;
        int consecutiveChecks = 0;
        int requiredConsecutiveChecks = 3;

        while (System.currentTimeMillis() < deadline) {
            String actualValue = replicaStore.get(lastKey);
            if (expectedValue.equals(actualValue)) {
                consecutiveChecks++;
                if (consecutiveChecks >= requiredConsecutiveChecks) {
                    return true;
                }
            } else {
                consecutiveChecks = 0;
            }
            Thread.sleep(50);
        }
        return false;
    }
}