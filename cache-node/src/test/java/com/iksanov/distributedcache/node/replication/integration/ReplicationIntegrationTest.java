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
 * Verifies propagation of SET and DELETE commands and ignores self-origin messages.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ReplicationIntegrationTest {

    private NioEventLoopGroup bossGroup;
    private NioEventLoopGroup workerGroup;
    private ReplicationManager masterReplicationManager;
    private ReplicationManager replicaReplicationManager;
    private CacheStore masterStore;
    private CacheStore replicaStore;
    private NodeInfo masterNode;
    private NodeInfo replicaNode;
    private ReplicaManager replicaManager;
    private ReplicationReceiver replicaReceiver;

    @BeforeAll
    void setup() throws Exception {
        bossGroup = new NioEventLoopGroup(1);
        workerGroup = new NioEventLoopGroup(2);

        int masterPort = 9500;
        int replicaPort = 9501;

        masterNode = new NodeInfo("master-1", "127.0.0.1", masterPort);
        replicaNode = new NodeInfo("replica-1", "127.0.0.1", replicaPort);

        masterStore = new InMemoryCacheStore(1000, 0, 500);
        replicaStore = new InMemoryCacheStore(1000, 0, 500);

        // 🔹 Важно: слушаем именно replicationPort (port + 1000)
        replicaReceiver = new ReplicationReceiver(
                replicaNode.host(),
                replicaNode.replicationPort(),
                replicaStore
        );
        replicaReceiver.start();

        replicaManager = new ReplicaManager();
        replicaManager.registerReplica(masterNode, replicaNode);

        // 🔹 Передаём общий EventLoopGroup, чтобы избежать гонок
        ReplicationSender masterSender = new ReplicationSender(replicaManager, workerGroup);
        ReplicationSender replicaSender = new ReplicationSender(replicaManager, workerGroup);

        // 🔹 Мастер тоже слушает на своём replicationPort
        ReplicationReceiver masterReceiver = new ReplicationReceiver(
                masterNode.host(),
                masterNode.replicationPort(),
                masterStore
        );
        masterReceiver.start();

        Function<String, NodeInfo> masterPrimaryResolver = k -> masterNode;
        Function<String, NodeInfo> replicaPrimaryResolver = k -> masterNode;

        masterReplicationManager = new ReplicationManager(masterNode, masterSender, masterReceiver, masterPrimaryResolver);
        replicaReplicationManager = new ReplicationManager(replicaNode, replicaSender, replicaReceiver, replicaPrimaryResolver);

        // Небольшая задержка для Netty init
        Thread.sleep(300);
    }

    @AfterAll
    void tearDown() {
        if (masterReplicationManager != null) masterReplicationManager.shutdown();
        if (replicaReplicationManager != null) replicaReplicationManager.shutdown();
        if (replicaReceiver != null) replicaReceiver.stop();
        if (bossGroup != null) bossGroup.shutdownGracefully();
        if (workerGroup != null) workerGroup.shutdownGracefully();
    }

    @Test
    void testReplicationSetAndDelete() throws Exception {
        String key = "foo";
        String value = "bar";

        masterReplicationManager.onLocalSet(key, value);

        awaitCondition(() -> value.equals(replicaStore.get(key)), Duration.ofSeconds(5), "replicated value");

        assertEquals(value, replicaStore.get(key), "Replica must receive value via replication");

        masterReplicationManager.onLocalDelete(key);

        awaitCondition(() -> replicaStore.get(key) == null, Duration.ofSeconds(5), "replicated delete");

        assertNull(replicaStore.get(key), "Replica must remove value via replication delete");
    }

    @Test
    void testSelfOriginIgnored() {
        String key = "loopKey";
        String value = "v";

        ReplicationTask task = ReplicationTask.ofSet(key, value, replicaNode.nodeId());

        replicaReceiver.applyTask(task);

        assertNull(replicaStore.get(key), "Self-origin replication task must be ignored");
    }

    private void awaitCondition(java.util.function.Supplier<Boolean> cond, Duration timeout, String what) throws InterruptedException {
        Instant start = Instant.now();
        while (Duration.between(start, Instant.now()).compareTo(timeout) < 0) {
            try {
                if (Boolean.TRUE.equals(cond.get())) return;
            } catch (Throwable ignored) {}
            TimeUnit.MILLISECONDS.sleep(100);
        }
        fail("Timed out waiting for " + what);
    }
}
