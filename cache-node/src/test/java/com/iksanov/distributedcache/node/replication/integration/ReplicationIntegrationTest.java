package com.iksanov.distributedcache.node.replication.integration;

import com.iksanov.distributedcache.common.cluster.NodeInfo;
import com.iksanov.distributedcache.common.cluster.ReplicaManager;
import com.iksanov.distributedcache.node.core.CacheStore;
import com.iksanov.distributedcache.node.core.InMemoryCacheStore;
import com.iksanov.distributedcache.node.replication.*;
import io.netty.channel.nio.NioEventLoopGroup;
import org.junit.jupiter.api.*;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test for replication between a master and a single replica node.
 * Verifies propagation of SET and DELETE commands.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ReplicationIntegrationTest {

    private NioEventLoopGroup sharedEventLoopGroup;
    private ReplicationManager masterReplicationManager;
    private ReplicationManager replicaReplicationManager;
    private CacheStore masterStore;
    private CacheStore replicaStore;
    private NodeInfo masterNode;
    private NodeInfo replicaNode;
    private ReplicaManager replicaManager;
    private ReplicationReceiver masterReceiver;
    private ReplicationReceiver replicaReceiver;
    private ReplicationSender masterSender;
    private ReplicationSender replicaSender;

    @BeforeAll
    void setup() throws Exception {
        sharedEventLoopGroup = new NioEventLoopGroup(2);

        masterNode = new NodeInfo("master-1", "127.0.0.1", 9500);
        replicaNode = new NodeInfo("replica-1", "127.0.0.1", 9501);

        masterStore = new InMemoryCacheStore(1000, 0, 500);
        replicaStore = new InMemoryCacheStore(1000, 0, 500);

        masterReceiver = new ReplicationReceiver(
                masterNode.host(),
                masterNode.replicationPort(),
                masterStore
        );
        masterReceiver.start();

        replicaReceiver = new ReplicationReceiver(
                replicaNode.host(),
                replicaNode.replicationPort(),
                replicaStore
        );
        replicaReceiver.start();

        Thread.sleep(500);

        replicaManager = new ReplicaManager();
        replicaManager.registerReplica(masterNode, replicaNode);
        masterSender = new ReplicationSender(replicaManager, sharedEventLoopGroup);
        replicaSender = new ReplicationSender(replicaManager, sharedEventLoopGroup);

        Function<String, NodeInfo> masterPrimaryResolver = k -> masterNode;
        Function<String, NodeInfo> replicaPrimaryResolver = k -> masterNode;

        masterReplicationManager = new ReplicationManager(
                masterNode,
                masterSender,
                masterReceiver,
                masterPrimaryResolver
        );

        replicaReplicationManager = new ReplicationManager(
                replicaNode,
                replicaSender,
                replicaReceiver,
                replicaPrimaryResolver
        );
        Thread.sleep(300);
    }

    @AfterAll
    void tearDown() {
        if (masterReplicationManager != null) {
            masterReplicationManager.shutdown();
        }
        if (replicaReplicationManager != null) {
            replicaReplicationManager.shutdown();
        }
        if (masterReceiver != null) {
            masterReceiver.stop();
        }
        if (replicaReceiver != null) {
            replicaReceiver.stop();
        }
        if (sharedEventLoopGroup != null) {
            sharedEventLoopGroup.shutdownGracefully();
        }
    }

    @Test
    @DisplayName("Should replicate SET operation from master to replica")
    void testReplicationSet() throws Exception {
        String key = "user:100";
        String value = "Alice";

        masterReplicationManager.onLocalSet(key, value);

        awaitCondition(
                () -> value.equals(replicaStore.get(key)),
                Duration.ofSeconds(5),
                "replicated SET operation"
        );

        assertEquals(value, replicaStore.get(key), "Replica must receive value via replication");
    }

    @Test
    @DisplayName("Should replicate DELETE operation from master to replica")
    void testReplicationDelete() throws Exception {
        String key = "session:42";
        String value = "active";

        masterReplicationManager.onLocalSet(key, value);
        awaitCondition(
                () -> value.equals(replicaStore.get(key)),
                Duration.ofSeconds(5),
                "initial SET replication"
        );
        masterReplicationManager.onLocalDelete(key);
        awaitCondition(
                () -> replicaStore.get(key) == null,
                Duration.ofSeconds(5),
                "replicated DELETE operation"
        );
        assertNull(replicaStore.get(key), "Replica must remove value via replication delete");
    }

    @Test
    @DisplayName("Should handle multiple sequential operations correctly")
    void testMultipleSequentialOperations() throws Exception {
        String key = "counter";

        for (int i = 0; i < 5; i++) {
            String value = "v" + i;
            masterReplicationManager.onLocalSet(key, value);

            String expectedValue = value;
            awaitCondition(
                    () -> expectedValue.equals(replicaStore.get(key)),
                    Duration.ofSeconds(3),
                    "replication of iteration " + i
            );

            assertEquals(value, replicaStore.get(key),
                    "Replica should have latest value after iteration " + i);
        }
    }

    @Test
    @DisplayName("Should replicate multiple different keys")
    void testMultipleKeys() throws Exception {
        int keyCount = 10;

        // Replicate multiple keys
        for (int i = 0; i < keyCount; i++) {
            String key = "key-" + i;
            String value = "value-" + i;
            masterReplicationManager.onLocalSet(key, value);
        }

        // Verify all keys are replicated
        for (int i = 0; i < keyCount; i++) {
            String key = "key-" + i;
            String expectedValue = "value-" + i;

            awaitCondition(
                    () -> expectedValue.equals(replicaStore.get(key)),
                    Duration.ofSeconds(5),
                    "replication of " + key
            );

            assertEquals(expectedValue, replicaStore.get(key),
                    "Key " + key + " must be replicated correctly");
        }
    }

    @Test
    @DisplayName("Replica should not replicate when it's not the master for a key")
    void testReplicaDoesNotReplicateNonMasterKeys() throws Exception {
        String key = "replica-key";
        String value = "replica-value";

        int replicaStoreSizeBefore = replicaStore.size();

        // Replica tries to perform local set (but it's not master for this key)
        replicaReplicationManager.onLocalSet(key, value);

        // Wait a bit to ensure no replication happens
        Thread.sleep(500);

        // Master should NOT have received this (replica shouldn't replicate)
        assertNull(masterStore.get(key),
                "Master should not receive replication from replica when replica is not master");
    }

    /**
     * Helper method to wait for a condition with timeout.
     *
     * @param condition the condition to check
     * @param timeout   maximum time to wait
     * @param what      description for error messages
     */
    private void awaitCondition(
            java.util.function.Supplier<Boolean> condition,
            Duration timeout,
            String what
    ) throws InterruptedException {
        Instant start = Instant.now();
        while (Duration.between(start, Instant.now()).compareTo(timeout) < 0) {
            try {
                if (Boolean.TRUE.equals(condition.get())) {
                    return;
                }
            } catch (Throwable ignored) {
                // Continue waiting
            }
            TimeUnit.MILLISECONDS.sleep(50);
        }
        fail("Timed out waiting for " + what);
    }
}