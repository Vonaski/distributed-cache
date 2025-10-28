package com.iksanov.distributedcache.common.cluster.sharding;

import com.iksanov.distributedcache.common.cluster.NodeInfo;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive tests for {@link ConsistentHashRing}.
 * Ensures correct node management, consistent distribution, stability, and error handling.
 */
class ConsistentHashRingTest {

    private ConsistentHashRing ring;
    private NodeInfo nodeA, nodeB, nodeC, nodeD;

    @BeforeEach
    void setup() {
        ring = new ConsistentHashRing(100);
        nodeA = new NodeInfo("A", "10.0.0.1", 8081);
        nodeB = new NodeInfo("B", "10.0.0.2", 8082);
        nodeC = new NodeInfo("C", "10.0.0.3", 8083);
        nodeD = new NodeInfo("D", "10.0.0.4", 8084);
        ring.addNode(nodeA);
        ring.addNode(nodeB);
        ring.addNode(nodeC);
    }

    @Test
    @DisplayName("getNodeForKey() should always return a node when the ring is not empty")
    void testGetNodeForKeyAlwaysReturnsNode() {
        for (int i = 0; i < 1000; i++) {
            NodeInfo node = ring.getNodeForKey("key-" + i);
            assertNotNull(node, "Expected non-null node for key-" + i);
        }
    }

    @Test
    @DisplayName("Add and remove operations should correctly update the ring")
    void testAddAndRemoveNode() {
        assertEquals(3, ring.getNodes().size());

        ring.addNode(nodeD);
        assertTrue(ring.getNodes().contains(nodeD));
        assertEquals(4, ring.getNodes().size());

        ring.removeNode(nodeB);
        assertFalse(ring.getNodes().contains(nodeB));
        assertEquals(3, ring.getNodes().size());
    }

    @Test
    @DisplayName("Keys should be approximately evenly distributed among nodes")
    void testDistributionIsReasonablyBalanced() {
        Map<String, Integer> counts = new HashMap<>();
        int totalKeys = 10_000;
        for (int i = 0; i < totalKeys; i++) {
            NodeInfo n = ring.getNodeForKey("key-" + i);
            counts.merge(n.nodeId(), 1, Integer::sum);
        }

        double average = totalKeys / (double) ring.getNodes().size();
        counts.forEach((id, count) -> {
            double deviation = Math.abs(count - average) / average;
            assertTrue(deviation < 0.4, // allow up to 40% deviation
                    "Node " + id + " distribution is too unbalanced, deviation=" + deviation);
        });
    }

    @Test
    @DisplayName("Hashing should be stable for identical rings")
    void testHashStabilityAcrossIdenticalRings() {
        ConsistentHashRing ring2 = new ConsistentHashRing(100);
        ring2.addNode(nodeA);
        ring2.addNode(nodeB);
        ring2.addNode(nodeC);

        for (int i = 0; i < 1000; i++) {
            String key = "key-" + i;
            NodeInfo n1 = ring.getNodeForKey(key);
            NodeInfo n2 = ring2.getNodeForKey(key);
            assertEquals(n1, n2, "Same ring configuration must yield identical key mapping for " + key);
        }
    }

    @Test
    @DisplayName("Adding a new node should cause minimal key reassignment (consistent property)")
    void testMinimalKeyReassignmentAfterAddingNode() {
        Map<String, NodeInfo> before = new HashMap<>();
        for (int i = 0; i < 5000; i++) {
            String key = "key-" + i;
            before.put(key, ring.getNodeForKey(key));
        }

        ring.addNode(nodeD);

        int moved = 0;
        for (int i = 0; i < 5000; i++) {
            String key = "key-" + i;
            NodeInfo afterNode = ring.getNodeForKey(key);
            if (!afterNode.equals(before.get(key))) {
                moved++;
            }
        }

        double movedRatio = moved / 5000.0;
        // For consistent hashing, only a fraction of keys should move (~1/N)
        assertTrue(movedRatio < 0.5, "Too many keys were remapped after adding one node: " + movedRatio);
    }

    @Test
    @DisplayName("Removing all nodes should make the ring empty and return null on lookup")
    void testEmptyRingReturnsNull() {
        ring.removeNode(nodeA);
        ring.removeNode(nodeB);
        ring.removeNode(nodeC);

        assertNull(ring.getNodeForKey("any-key"));
        assertEquals(0, ring.getNodes().size());
    }

    @Test
    @DisplayName("Adding the same node multiple times should not create duplicates")
    void testDuplicateNodeIsIgnored() {
        int before = ring.getNodes().size();
        ring.addNode(nodeA); // already exists
        assertEquals(before, ring.getNodes().size(),
                "Adding a duplicate node must not increase the node count");
    }

    @Test
    @DisplayName("getNodeForKey() should throw NPE if key is null")
    void testGetNodeForKeyThrowsOnNullKey() {
        assertThrows(NullPointerException.class, () -> ring.getNodeForKey(null));
    }

    @Test
    @DisplayName("addNode() and removeNode() should throw NPE when node is null")
    void testAddRemoveNullNodeThrows() {
        assertThrows(NullPointerException.class, () -> ring.addNode(null));
        assertThrows(NullPointerException.class, () -> ring.removeNode(null));
    }

    @Test
    @DisplayName("snapshot() should return an unmodifiable view of the internal map")
    void testSnapshotIsUnmodifiable() {
        var snapshot = ring.snapshot();
        assertThrows(UnsupportedOperationException.class, snapshot::clear);
    }
}
