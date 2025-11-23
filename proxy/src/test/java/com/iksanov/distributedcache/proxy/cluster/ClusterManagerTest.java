package com.iksanov.distributedcache.proxy.cluster;

import com.iksanov.distributedcache.common.cluster.NodeInfo;
import com.iksanov.distributedcache.proxy.config.CacheClusterConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import java.util.List;
import java.util.Set;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link ClusterManager}.
 * <p>
 * Tests cluster initialization, node routing, and dynamic master management.
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("ClusterManager Tests")
class ClusterManagerTest {

    @Mock
    private CacheClusterConfig config;

    private ClusterManager clusterManager;

    @BeforeEach
    void setUp() {
        when(config.getVirtualNodes()).thenReturn(150);
    }

    @Test
    @DisplayName("Should initialize cluster with master and replica nodes")
    void shouldInitializeClusterWithMasterAndReplica() {
        List<String> nodes = List.of(
                "master-1:localhost:7000:7100",
                "replica-1-master-1:localhost:7001:7101"
        );
        when(config.getNodes()).thenReturn(nodes);
        clusterManager = new ClusterManager(config);
        clusterManager.init();
        Set<NodeInfo> allNodes = clusterManager.getAllNodes();
        assertEquals(1, allNodes.size());
        NodeInfo master = allNodes.iterator().next();
        assertEquals("master-1", master.nodeId());
        NodeInfo masterInfo = new NodeInfo("master-1", "localhost", 7000, 7100);
        Set<NodeInfo> replicas = clusterManager.getReplicas(masterInfo);
        assertEquals(1, replicas.size());
        NodeInfo replica = replicas.iterator().next();
        assertEquals("replica-1-master-1", replica.nodeId());
    }

    @Test
    @DisplayName("Should initialize cluster with multiple shards")
    void shouldInitializeClusterWithMultipleShards() {
        List<String> nodes = List.of(
                "master-1:localhost:7000:7100",
                "replica-1-master-1:localhost:7001:7101",
                "master-2:localhost:7002:7102",
                "replica-1-master-2:localhost:7003:7103"
        );
        when(config.getNodes()).thenReturn(nodes);
        clusterManager = new ClusterManager(config);
        clusterManager.init();
        Set<NodeInfo> allMasters = clusterManager.getAllNodes();
        assertEquals(2, allMasters.size());
        NodeInfo master1 = new NodeInfo("master-1", "localhost", 7000, 7100);
        Set<NodeInfo> replicas1 = clusterManager.getReplicas(master1);
        assertEquals(1, replicas1.size());
        assertEquals("replica-1-master-1", replicas1.iterator().next().nodeId());
        NodeInfo master2 = new NodeInfo("master-2", "localhost", 7002, 7102);
        Set<NodeInfo> replicas2 = clusterManager.getReplicas(master2);
        assertEquals(1, replicas2.size());
        assertEquals("replica-1-master-2", replicas2.iterator().next().nodeId());
    }

    @Test
    @DisplayName("Should throw exception when no nodes configured")
    void shouldThrowExceptionWhenNoNodesConfigured() {
        when(config.getNodes()).thenReturn(List.of());
        clusterManager = new ClusterManager(config);
        assertThrows(IllegalStateException.class, () -> clusterManager.init());
    }

    @Test
    @DisplayName("Should get master node for key using consistent hashing")
    void shouldGetMasterNodeForKey() {
        List<String> nodes = List.of(
                "master-1:localhost:7000:7100",
                "master-2:localhost:7001:7101",
                "master-3:localhost:7002:7102"
        );
        when(config.getNodes()).thenReturn(nodes);
        clusterManager = new ClusterManager(config);
        clusterManager.init();
        NodeInfo node1 = clusterManager.getMasterNodeForKey("user:1");
        NodeInfo node2 = clusterManager.getMasterNodeForKey("user:1");
        assertNotNull(node1);
        assertEquals(node1, node2);
        NodeInfo node3 = clusterManager.getMasterNodeForKey("user:2");
        assertNotNull(node3);
    }

    @Test
    @DisplayName("Should get node for read from master and replicas")
    void shouldGetNodeForReadFromMasterAndReplicas() {
        List<String> nodes = List.of(
                "master-1:localhost:7000:7100",
                "replica-1-master-1:localhost:7001:7101"
        );
        when(config.getNodes()).thenReturn(nodes);
        clusterManager = new ClusterManager(config);
        clusterManager.init();
        NodeInfo readNode = clusterManager.getNodeForRead("user:1");
        assertNotNull(readNode);
        assertTrue(readNode.nodeId().equals("master-1") || readNode.nodeId().equals("replica-1-master-1"));
    }

    @Test
    @DisplayName("Should use master for read when no replicas available")
    void shouldUseMasterForReadWhenNoReplicas() {
        List<String> nodes = List.of("master-1:localhost:7000:7100");
        when(config.getNodes()).thenReturn(nodes);
        clusterManager = new ClusterManager(config);
        clusterManager.init();
        NodeInfo readNode = clusterManager.getNodeForRead("user:1");
        assertNotNull(readNode);
        assertEquals("master-1", readNode.nodeId());
    }

    @Test
    @DisplayName("Should remove master from hash ring")
    void shouldRemoveMasterFromHashRing() {
        List<String> nodes = List.of("master-1:localhost:7000:7100", "master-2:localhost:7001:7101");
        when(config.getNodes()).thenReturn(nodes);
        clusterManager = new ClusterManager(config);
        clusterManager.init();
        assertEquals(2, clusterManager.getAllNodes().size());
        NodeInfo master1 = new NodeInfo("master-1", "localhost", 7000, 7100);
        clusterManager.removeMaster(master1);
        assertEquals(1, clusterManager.getAllNodes().size());
        assertFalse(clusterManager.getAllNodes().contains(master1));
    }

    @Test
    @DisplayName("Should add promoted replica as new master")
    void shouldAddPromotedReplicaAsNewMaster() {
        List<String> nodes = List.of("master-1:localhost:7000:7100", "replica-1-master-1:localhost:7001:7101");
        when(config.getNodes()).thenReturn(nodes);
        clusterManager = new ClusterManager(config);
        clusterManager.init();
        assertEquals(1, clusterManager.getAllNodes().size());
        NodeInfo replica = new NodeInfo("replica-1-master-1", "localhost", 7001, 7101);
        clusterManager.addMaster(replica);
        assertEquals(2, clusterManager.getAllNodes().size());
        assertTrue(clusterManager.getAllNodes().stream().anyMatch(n -> n.nodeId().equals("replica-1-master-1")));
    }

    @Test
    @DisplayName("Should handle null master in removeMaster gracefully")
    void shouldHandleNullMasterInRemoveMaster() {
        List<String> nodes = List.of("master-1:localhost:7000:7100");
        when(config.getNodes()).thenReturn(nodes);
        clusterManager = new ClusterManager(config);
        clusterManager.init();
        assertDoesNotThrow(() -> clusterManager.removeMaster(null));
        assertEquals(1, clusterManager.getAllNodes().size());
    }

    @Test
    @DisplayName("Should handle null master in addMaster gracefully")
    void shouldHandleNullMasterInAddMaster() {
        List<String> nodes = List.of("master-1:localhost:7000:7100");
        when(config.getNodes()).thenReturn(nodes);
        clusterManager = new ClusterManager(config);
        clusterManager.init();
        assertDoesNotThrow(() -> clusterManager.addMaster(null));
        assertEquals(1, clusterManager.getAllNodes().size());
    }

    @Test
    @DisplayName("Should restore recovered master")
    void shouldRestoreRecoveredMaster() {
        List<String> nodes = List.of("master-1:localhost:7000:7100", "master-2:localhost:7001:7101");
        when(config.getNodes()).thenReturn(nodes);
        clusterManager = new ClusterManager(config);
        clusterManager.init();
        NodeInfo master1 = new NodeInfo("master-1", "localhost", 7000, 7100);
        clusterManager.removeMaster(master1);
        assertEquals(1, clusterManager.getAllNodes().size());
        clusterManager.restoreMaster(master1);
        assertEquals(2, clusterManager.getAllNodes().size());
        assertTrue(clusterManager.getAllNodes().contains(master1));
    }

    @Test
    @DisplayName("Should extract master nodeId from replica nodeId")
    void shouldExtractMasterNodeIdFromReplicaNodeId() {
        List<String> nodes = List.of("shard-1:localhost:7000:7100", "replica-1-shard-1:localhost:7001:7101");
        when(config.getNodes()).thenReturn(nodes);
        clusterManager = new ClusterManager(config);
        clusterManager.init();
        NodeInfo master = new NodeInfo("shard-1", "localhost", 7000, 7100);
        Set<NodeInfo> replicas = clusterManager.getReplicas(master);
        assertEquals(1, replicas.size());
        assertEquals("replica-1-shard-1", replicas.iterator().next().nodeId());
    }

    @Test
    @DisplayName("Should handle multiple replicas for same master")
    void shouldHandleMultipleReplicasForSameMaster() {
        List<String> nodes = List.of("master-1:localhost:7000:7100", "replica-1-master-1:localhost:7001:7101", "replica-2-master-1:localhost:7002:7102");
        when(config.getNodes()).thenReturn(nodes);
        clusterManager = new ClusterManager(config);
        clusterManager.init();
        NodeInfo master = new NodeInfo("master-1", "localhost", 7000, 7100);
        Set<NodeInfo> replicas = clusterManager.getReplicas(master);
        assertEquals(2, replicas.size());
    }

    @Test
    @DisplayName("Should warn when replica has no corresponding master")
    void shouldWarnWhenReplicaHasNoMaster() {
        List<String> nodes = List.of("master-1:localhost:7000:7100", "replica-1-master-2:localhost:7001:7101");
        when(config.getNodes()).thenReturn(nodes);
        clusterManager = new ClusterManager(config);
        clusterManager.init();
        NodeInfo master1 = new NodeInfo("master-1", "localhost", 7000, 7100);
        Set<NodeInfo> replicas = clusterManager.getReplicas(master1);
        assertEquals(0, replicas.size());
    }

    @Test
    @DisplayName("Should return empty set for replicas of unknown master")
    void shouldReturnEmptySetForReplicasOfUnknownMaster() {
        List<String> nodes = List.of("master-1:localhost:7000:7100");
        when(config.getNodes()).thenReturn(nodes);
        clusterManager = new ClusterManager(config);
        clusterManager.init();
        NodeInfo unknownMaster = new NodeInfo("master-999", "localhost", 9999, 9999);
        Set<NodeInfo> replicas = clusterManager.getReplicas(unknownMaster);
        assertNotNull(replicas);
        assertTrue(replicas.isEmpty());
    }

    @Test
    @DisplayName("Should distribute keys across multiple masters using consistent hashing")
    void shouldDistributeKeysAcrossMultipleMasters() {
        List<String> nodes = List.of("master-1:localhost:7000:7100", "master-2:localhost:7001:7101", "master-3:localhost:7002:7102");
        when(config.getNodes()).thenReturn(nodes);
        clusterManager = new ClusterManager(config);
        clusterManager.init();
        Set<String> masterNodeIds = new java.util.HashSet<>();
        for (int i = 0; i < 100; i++) {
            NodeInfo master = clusterManager.getMasterNodeForKey("key-" + i);
            masterNodeIds.add(master.nodeId());
        }
        assertTrue(masterNodeIds.size() >= 2, "Keys should be distributed across multiple masters");
    }

    @Test
    @DisplayName("Should be thread-safe for concurrent master removal and addition")
    void shouldBeThreadSafeForConcurrentOperations() throws InterruptedException {
        List<String> nodes = List.of("master-1:localhost:7000:7100");
        when(config.getNodes()).thenReturn(nodes);
        clusterManager = new ClusterManager(config);
        clusterManager.init();
        NodeInfo master1 = new NodeInfo("master-1", "localhost", 7000, 7100);
        NodeInfo replica1 = new NodeInfo("replica-1-master-1", "localhost", 7001, 7101);
        Thread thread1 = new Thread(() -> {
            for (int i = 0; i < 100; i++) {
                clusterManager.removeMaster(master1);
                clusterManager.addMaster(master1);
            }
        });
        Thread thread2 = new Thread(() -> {
            for (int i = 0; i < 100; i++) {
                clusterManager.addMaster(replica1);
                clusterManager.removeMaster(replica1);
            }
        });
        thread1.start();
        thread2.start();
        thread1.join();
        thread2.join();
        assertNotNull(clusterManager.getAllNodes());
    }

    @Test
    @DisplayName("Should return virtual node count from config")
    void shouldReturnVirtualNodeCountFromConfig() {
        when(config.getVirtualNodes()).thenReturn(250);
        when(config.getNodes()).thenReturn(List.of("master-1:localhost:7000:7100"));
        clusterManager = new ClusterManager(config);
        clusterManager.init();
        assertEquals(250, clusterManager.getVirtualNodeCount());
    }
}