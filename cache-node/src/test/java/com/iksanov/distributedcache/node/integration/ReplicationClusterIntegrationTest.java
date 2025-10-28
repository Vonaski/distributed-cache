//package com.iksanov.distributedcache.node.integration;
//
//import com.iksanov.distributedcache.common.cluster.NodeInfo;
//import com.iksanov.distributedcache.common.cluster.ReplicaManager;
//import com.iksanov.distributedcache.node.config.NetServerConfig;
//import com.iksanov.distributedcache.node.core.InMemoryCacheStore;
//import com.iksanov.distributedcache.node.net.NetServer;
//import com.iksanov.distributedcache.node.replication.ReplicationManager;
//import com.iksanov.distributedcache.node.replication.ReplicationReceiver;
//import com.iksanov.distributedcache.node.replication.ReplicationSender;
//import org.junit.jupiter.api.*;
//
//import java.util.*;
//import java.util.concurrent.TimeUnit;
//import java.util.function.Function;
//
//import static org.junit.jupiter.api.Assertions.*;
//
///**
// * Full cluster integration test across three nodes.
// * Verifies that replication works end-to-end (SET and DELETE).
// */
//@TestInstance(TestInstance.Lifecycle.PER_CLASS)
//class ReplicationClusterIntegrationTest {
//
//    private final List<NodeInfo> nodes = new ArrayList<>();
//    private final Map<String, InMemoryCacheStore> stores = new HashMap<>();
//    private final Map<String, ReplicationManager> managers = new HashMap<>();
//    private final Map<String, NetServer> servers = new HashMap<>();
//
//    @BeforeAll
//    void setUpCluster() throws Exception {
//        NodeInfo nodeA = new NodeInfo("nodeA", "127.0.0.1", 9101, 9201);
//        NodeInfo nodeB = new NodeInfo("nodeB", "127.0.0.1", 9102, 9202);
//        NodeInfo nodeC = new NodeInfo("nodeC", "127.0.0.1", 9103, 9203);
//        nodes.add(nodeA);
//        nodes.add(nodeB);
//        nodes.add(nodeC);
//
//        ReplicaManager commonReplicaManager = new ReplicaManager();
//        commonReplicaManager.registerReplica(nodeA, nodeB);
//        commonReplicaManager.registerReplica(nodeA, nodeC);
//        commonReplicaManager.registerReplica(nodeB, nodeA);
//        commonReplicaManager.registerReplica(nodeB, nodeC);
//        commonReplicaManager.registerReplica(nodeC, nodeA);
//        commonReplicaManager.registerReplica(nodeC, nodeB);
//
//        Function<String, NodeInfo> primaryResolver = (k) -> nodeA;
//
//        for (NodeInfo node : nodes) {
//            InMemoryCacheStore store = new InMemoryCacheStore(1000, 0, 1000);
//            stores.put(node.nodeId(), store);
//
//            ReplicationSender sender = new ReplicationSender(commonReplicaManager);
//            ReplicationReceiver receiver = new ReplicationReceiver(store, node.nodeId(), node.replicationPort());
//            ReplicationManager manager = new ReplicationManager(node, sender, receiver, primaryResolver);
//            managers.put(node.nodeId(), manager);
//
//            NetServerConfig cfg = new NetServerConfig(
//                    node.host(),
//                    node.port(),
//                    1, 1, 128,
//                    1024 * 1024,
//                    2, 10
//            );
//
//            NetServer netServer = new NetServer(cfg, store, manager);
//            netServer.start();
//            servers.put(node.nodeId(), netServer);
//        }
//
//        TimeUnit.SECONDS.sleep(1);
//    }
//
//    @AfterAll
//    void tearDownCluster() throws Exception {
//        for (ReplicationManager manager : managers.values()) {
//            try {
//                manager.shutdown();
//            } catch (Exception ignored) {
//            }
//        }
//        for (NetServer server : servers.values()) {
//            try {
//                server.stop();
//            } catch (Exception ignored) {
//            }
//        }
//        TimeUnit.SECONDS.sleep(1);
//    }
//
//    @Test
//    @DisplayName("E2E: value replicated to all nodes, then deleted everywhere")
//    void shouldReplicateSetAndDeleteAcrossAllNodes() throws Exception {
//        NodeInfo master = nodes.get(0); // nodeA
//        InMemoryCacheStore masterStore = stores.get(master.nodeId());
//        ReplicationManager masterManager = managers.get(master.nodeId());
//
//        String key = "foo-e2e";
//        String value = "bar-e2e";
//
//        masterStore.put(key, value);
//        masterManager.onLocalSet(key, value);
//
//        waitForReplication(key, value, nodes.size(), 5000);
//
//        for (NodeInfo node : nodes) {
//            String actual = stores.get(node.nodeId()).get(key);
//            assertEquals(value, actual, "Node " + node.nodeId() + " did not receive replicated value");
//        }
//
//        masterStore.delete(key);
//        masterManager.onLocalDelete(key);
//
//        waitForDeletion(key, nodes.size(), 5000);
//
//        for (NodeInfo node : nodes) {
//            assertNull(stores.get(node.nodeId()).get(key), "Node " + node.nodeId() + " still contains deleted key");
//        }
//    }
//
//    private void waitForReplication(String key, String expectedValue, int expectedNodes, long timeoutMs) throws InterruptedException {
//        long start = System.currentTimeMillis();
//        while (System.currentTimeMillis() - start < timeoutMs) {
//            long replicated = stores.values().stream()
//                    .filter(s -> expectedValue.equals(s.get(key)))
//                    .count();
//            if (replicated >= expectedNodes) return;
//            Thread.sleep(100);
//        }
//        fail("Timeout waiting for replication of key=" + key);
//    }
//
//    private void waitForDeletion(String key, int expectedNodes, long timeoutMs) throws InterruptedException {
//        long start = System.currentTimeMillis();
//        while (System.currentTimeMillis() - start < timeoutMs) {
//            long deleted = stores.values().stream()
//                    .filter(s -> s.get(key) == null)
//                    .count();
//            if (deleted >= expectedNodes) return;
//            Thread.sleep(100);
//        }
//        fail("Timeout waiting for deletion of key=" + key);
//    }
//}
