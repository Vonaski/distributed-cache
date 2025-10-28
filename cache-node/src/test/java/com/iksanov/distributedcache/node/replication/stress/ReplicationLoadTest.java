package com.iksanov.distributedcache.node.replication.stress;

import com.iksanov.distributedcache.common.cluster.NodeInfo;
import com.iksanov.distributedcache.common.cluster.ReplicaManager;
import com.iksanov.distributedcache.node.core.CacheStore;
import com.iksanov.distributedcache.node.core.InMemoryCacheStore;
import com.iksanov.distributedcache.node.replication.*;
import io.netty.channel.nio.NioEventLoopGroup;
import org.junit.jupiter.api.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Disabled("Manual start, performance benchmark")
public class ReplicationLoadTest {

    private static final int TOTAL_OPERATIONS = 100_000;
    private static final int BATCH_SIZE = 1_000;
    private static final long BATCH_WAIT_TIMEOUT_MS = 30_000;
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

    @BeforeAll
    void setup() throws Exception {
        sharedEventLoopGroup = new NioEventLoopGroup(4);

        masterNode = new NodeInfo("master-batch", "127.0.0.1", 9800);
        replicaNode = new NodeInfo("replica-batch", "127.0.0.1", 9801);

        masterStore = new InMemoryCacheStore(2_000_000, 0, 5_000);
        replicaStore = new InMemoryCacheStore(2_000_000, 0, 5_000);

        replicaReceiver = new ReplicationReceiver(replicaNode.host(), replicaNode.replicationPort(), replicaStore);
        replicaReceiver.start();

        Thread.sleep(1000);

        replicaManager = new ReplicaManager();
        replicaManager.registerReplica(masterNode, replicaNode);
        System.out.println("replicas for master: " + replicaManager.getReplicas(masterNode));

        sender = new ReplicationSender(replicaManager, sharedEventLoopGroup);

        masterReceiver = new ReplicationReceiver(masterNode.host(), masterNode.replicationPort(), masterStore);
        masterReceiver.start();

        Thread.sleep(500);

        masterReplicationManager = new ReplicationManager(masterNode, sender, masterReceiver, key -> masterNode);

        System.out.println("Replica receiver started, master replication initialized");

        System.out.println("Testing connectivity...");
        masterReplicationManager.onLocalSet("test-key", "test-value");
        Thread.sleep(1000);

        String testValue = replicaStore.get("test-key");
        if (testValue == null) {
            throw new IllegalStateException("Test connectivity failed - replica did not receive test message");
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
    void replicationBatchedBenchmark() throws Exception {
        System.out.printf("Starting batched replication test: total=%d, batch=%d%n", TOTAL_OPERATIONS, BATCH_SIZE);

        long startAll = System.nanoTime();
        List<Long> perOpLatenciesMicros = new ArrayList<>(TOTAL_OPERATIONS);

        int sent = 0;
        while (sent < TOTAL_OPERATIONS) {
            int toSend = Math.min(BATCH_SIZE, TOTAL_OPERATIONS - sent);
            int batchStart = sent;
            int batchEnd = sent + toSend - 1;

            long batchSendStart = System.nanoTime();
            for (int i = batchStart; i <= batchEnd; i++) {
                masterReplicationManager.onLocalSet("key-" + i, "val-" + i);
            }
            long batchSendEnd = System.nanoTime();
            double batchSendMs = (batchSendEnd - batchSendStart) / 1_000_000.0;
            System.out.printf("Batch %d..%d sent in %.2f ms%n", batchStart, batchEnd, batchSendMs);

            Thread.sleep(50);

            boolean ok = awaitReplicaProgress(batchEnd, BATCH_WAIT_TIMEOUT_MS);
            if (!ok) {
                System.err.printf("Timeout waiting for key-%d. Last successful key might be earlier.%n", batchEnd);

                for (int i = batchEnd; i >= batchStart; i--) {
                    if (replicaStore.get("key-" + i) != null) {
                        System.err.printf("Last replicated key found: key-%d%n", i);
                        break;
                    }
                }
                throw new AssertionError("Timeout waiting for replica to apply batch ending at " + batchEnd);
            }

            long batchCatchUpEnd = System.nanoTime();
            long batchCatchUpMicros = TimeUnit.NANOSECONDS.toMicros(batchCatchUpEnd - batchSendEnd);
            long avgPerOp = batchCatchUpMicros / toSend;
            for (int i = 0; i < toSend; i++) perOpLatenciesMicros.add(avgPerOp);

            sent += toSend;
            System.out.printf("Batch %d..%d applied. Total applied: %d%n", batchStart, batchEnd, sent);
        }

        long totalNs = System.nanoTime() - startAll;
        double totalSec = totalNs / 1_000_000_000.0;
        double opsPerSec = TOTAL_OPERATIONS / totalSec;
        Collections.sort(perOpLatenciesMicros);
        long avg = (long) perOpLatenciesMicros.stream().mapToLong(Long::longValue).average().orElse(0);
        long p95 = perOpLatenciesMicros.get((int)(perOpLatenciesMicros.size() * 0.95));

        System.out.println("=== Results ===");
        System.out.printf("Total time: %.2f s, throughput: %.2f ops/sec, avg latency: %d µs, p95: %d µs%n",
                totalSec, opsPerSec, avg, p95);

        assertEquals("val-" + (TOTAL_OPERATIONS - 1),
                replicaStore.get("key-" + (TOTAL_OPERATIONS - 1)),
                "last key must be present");
    }

    private boolean awaitReplicaProgress(int lastIndexNeeded, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        int consecutiveChecks = 0;
        while (System.currentTimeMillis() < deadline) {
            if (replicaStore.get("key-" + lastIndexNeeded) != null) {
                consecutiveChecks++;
                if (consecutiveChecks >= 3) {
                    return true;
                }
            } else {
                consecutiveChecks = 0;
            }
            Thread.sleep(10);
        }
        return false;
    }
}