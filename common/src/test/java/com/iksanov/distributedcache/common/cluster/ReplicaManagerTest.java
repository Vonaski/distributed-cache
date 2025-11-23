package com.iksanov.distributedcache.common.cluster;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive tests for {@link ReplicaManager}.
 * Ensures correct registration, lookup, removal, and consistency between master-replica mappings.
 */
class ReplicaManagerTest {

    private ReplicaManager replicaManager;
    private NodeInfo master1;
    private NodeInfo master2;
    private NodeInfo replica1;
    private NodeInfo replica2;
    private NodeInfo replica3;

    @BeforeEach
    void setup() {
        replicaManager = new ReplicaManager();
        master1 = new NodeInfo("master-1", "10.0.0.1", 8080);
        master2 = new NodeInfo("master-2", "10.0.0.2", 8081);
        replica1 = new NodeInfo("replica-1", "10.0.0.3", 8082);
        replica2 = new NodeInfo("replica-2", "10.0.0.4", 8083);
        replica3 = new NodeInfo("replica-3", "10.0.0.5", 8084);
    }

    @Test
    @DisplayName("Registering replicas should establish master-to-replica and replica-to-master links")
    void testRegisterReplicaCreatesBidirectionalMapping() {
        replicaManager.registerReplica(master1, replica1);
        replicaManager.registerReplica(master1, replica2);
        Set<NodeInfo> replicas = replicaManager.getReplicas(master1);
        assertEquals(2, replicas.size(), "Master should have exactly two replicas");
        assertTrue(replicas.contains(replica1));
        assertTrue(replicas.contains(replica2));
        assertEquals(master1, replicaManager.getMaster(replica1), "Replica1 should reference master1");
        assertEquals(master1, replicaManager.getMaster(replica2), "Replica2 should reference master1");
    }

    @Test
    @DisplayName("Removing a replica should remove both sides of the mapping")
    void testRemoveReplicaRemovesFromMasterAndReplicaMap() {
        replicaManager.registerReplica(master1, replica1);
        replicaManager.registerReplica(master1, replica2);
        replicaManager.removeReplica(replica1);
        assertNull(replicaManager.getMaster(replica1), "Replica1 should no longer have a master mapping");
        Set<NodeInfo> replicas = replicaManager.getReplicas(master1);
        assertFalse(replicas.contains(replica1), "Replica1 should be removed from master1’s replica set");
        assertTrue(replicas.contains(replica2), "Replica2 should still remain linked to master1");
    }

    @Test
    @DisplayName("Removing a master should clear all associated replicas")
    void testRemoveMasterRemovesAllAssociatedReplicas() {
        replicaManager.registerReplica(master1, replica1);
        replicaManager.registerReplica(master1, replica2);
        replicaManager.registerReplica(master2, replica3);
        replicaManager.removeMaster(master1);
        assertTrue(replicaManager.getReplicas(master1).isEmpty(), "All replicas of master1 should be removed");
        assertNull(replicaManager.getMaster(replica1), "Replica1 should no longer be linked to master1");
        assertNull(replicaManager.getMaster(replica2), "Replica2 should no longer be linked to master1");
        assertEquals(master2, replicaManager.getMaster(replica3));
        assertFalse(replicaManager.getReplicas(master2).isEmpty());
    }

    @Test
    @DisplayName("Different masters should maintain isolated replica sets")
    void testMultipleMastersRemainIsolated() {
        replicaManager.registerReplica(master1, replica1);
        replicaManager.registerReplica(master2, replica2);
        assertEquals(Set.of(replica1), replicaManager.getReplicas(master1));
        assertEquals(Set.of(replica2), replicaManager.getReplicas(master2));
        assertEquals(master1, replicaManager.getMaster(replica1));
        assertEquals(master2, replicaManager.getMaster(replica2));
        assertNotEquals(replicaManager.getMaster(replica1), replicaManager.getMaster(replica2));
    }

    @Test
    @DisplayName("Registering the same replica again should not create duplicates")
    void testDuplicateRegistrationIsIgnored() {
        replicaManager.registerReplica(master1, replica1);
        replicaManager.registerReplica(master1, replica1);
        Set<NodeInfo> replicas = replicaManager.getReplicas(master1);
        assertEquals(1, replicas.size(), "Duplicate replica registration should not create extra entries");
    }

    @Test
    @DisplayName("Removing unknown master or replica should not throw exceptions")
    void testRemovingUnknownMasterOrReplicaDoesNothing() {
        assertDoesNotThrow(() -> replicaManager.removeMaster(master1));
        assertDoesNotThrow(() -> replicaManager.removeReplica(replica1));
    }

    @Test
    @DisplayName("Registering or removing null nodes should throw NullPointerException")
    void testRegisterAndRemoveNullThrows() {
        assertThrows(NullPointerException.class, () -> replicaManager.registerReplica(null, replica1));
        assertThrows(NullPointerException.class, () -> replicaManager.registerReplica(master1, null));
        assertThrows(NullPointerException.class, () -> replicaManager.removeMaster(null));
        assertThrows(NullPointerException.class, () -> replicaManager.removeReplica(null));
    }

    @Test
    @DisplayName("ReplicaManager should support multiple replicas per master")
    void testReplicationFactorGreaterThanOne() {
        replicaManager.registerReplica(master1, replica1);
        replicaManager.registerReplica(master1, replica2);
        replicaManager.registerReplica(master1, replica3);
        Set<NodeInfo> replicas = replicaManager.getReplicas(master1);
        assertEquals(3, replicas.size());
        assertTrue(replicas.containsAll(Set.of(replica1, replica2, replica3)));
    }

    @Test
    @DisplayName("getMaster() should return null for unknown replicas")
    void testGetMasterReturnsNullForUnknownReplica() {
        assertNull(replicaManager.getMaster(replica1));
    }

    @Test
    void testConcurrentRegisterAndRemove() throws InterruptedException {
        ReplicaManager manager = new ReplicaManager();
        NodeInfo master = new NodeInfo("m1", "127.0.0.1", 8080);
        try (ExecutorService pool = Executors.newFixedThreadPool(4)) {
            for (int i = 0; i < 100; i++) {
                int idx = i;
                pool.submit(() -> {
                    NodeInfo replica = new NodeInfo("r" + idx, "127.0.0.1", 8000 + idx);
                    manager.registerReplica(master, replica);
                    if (idx % 2 == 0) manager.removeReplica(replica);
                });
            }
            pool.shutdown();
            pool.awaitTermination(3, TimeUnit.SECONDS);
        }
        for (NodeInfo replica : manager.getReplicas(master)) {
            assertEquals(master, manager.getMaster(replica));
        }
    }
}
