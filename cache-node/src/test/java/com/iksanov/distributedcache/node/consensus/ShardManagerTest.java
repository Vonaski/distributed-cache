package com.iksanov.distributedcache.node.consensus;

import com.iksanov.distributedcache.node.config.ClusterConfig;
import com.iksanov.distributedcache.node.config.RaftConfig;
import com.iksanov.distributedcache.node.config.ShardConfig;
import com.iksanov.distributedcache.node.consensus.sharding.ShardManager;
import com.iksanov.distributedcache.node.core.CacheStore;
import com.iksanov.distributedcache.node.metrics.RaftMetrics;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.lenient;

/**
 * Unit tests for {@link ShardManager}.
 * <p>
 * Covers:
 *  - Shard initialization and lifecycle
 *  - Node participation filtering
 *  - Consistent hash ring management
 *  - Shard selection for keys
 *  - Error handling during startup
 */
@ExtendWith(MockitoExtension.class)
class ShardManagerTest {

    @TempDir
    Path tempDir;

    @Mock
    private CacheStore cacheStore;
    @Mock
    private RaftMetrics metrics;

    private ShardManager shardManager;
    private RaftConfig raftConfig;
    private final String nodeId = "node-1";
    private final String host = "localhost";

    @BeforeEach
    void setUp() {
        // Setup lenient mocks for RaftMetrics (RaftNode calls these in constructor and during operation)
        lenient().doNothing().when(metrics).setCurrentTerm(anyLong());
        lenient().doNothing().when(metrics).setLogSize(anyLong());
        lenient().doNothing().when(metrics).setCommitIndex(anyLong());
        lenient().doNothing().when(metrics).setNodeState(anyInt());
        lenient().doNothing().when(metrics).setLastApplied(anyLong());
        lenient().doNothing().when(metrics).incrementCommandsApplied();
        lenient().doNothing().when(metrics).incrementElectionsStarted(anyLong());
        lenient().doNothing().when(metrics).incrementElectionsWon(anyLong());
        lenient().doNothing().when(metrics).incrementElectionsFailed();
        lenient().doNothing().when(metrics).incrementStepDowns(anyLong());
        lenient().doNothing().when(metrics).incrementVoteGranted();
        lenient().doNothing().when(metrics).incrementVoteDenied();
        lenient().doNothing().when(metrics).incrementAppendEntriesSent();
        lenient().doNothing().when(metrics).incrementAppendEntriesSuccess();
        lenient().doNothing().when(metrics).incrementAppendEntriesFailed();
        lenient().doNothing().when(metrics).incrementHeartbeatsSent();
        lenient().doNothing().when(metrics).incrementCommandsReplicated();
        lenient().doNothing().when(metrics).incrementCommandsFailed();

        raftConfig = RaftConfig.defaults();
        shardManager = new ShardManager(nodeId, host, cacheStore, metrics, raftConfig, 128);
    }

    @org.junit.jupiter.api.AfterEach
    void tearDown() {
        // Stop all RaftNodes to prevent resource leaks
        if (shardManager != null) {
            try {
                shardManager.stopAll();
            } catch (Exception e) {
                // Ignore cleanup errors
            }
        }
    }

    @Test
    @DisplayName("Should initialize with empty shards")
    void shouldInitializeWithEmptyShards() {
        Set<String> localShards = shardManager.localShardIds();
        assertTrue(localShards.isEmpty(), "Initially should have no local shards");
    }

    @Test
    @DisplayName("Should start shard when node participates")
    void shouldStartShardWhenNodeParticipates() {
        ShardConfig shard1 = new ShardConfig(
                "shard-0",
                7100,
                List.of("node-1:localhost:7100", "node-2:localhost:7100", "node-3:localhost:7100")
        );

        ClusterConfig clusterConfig = new ClusterConfig(List.of(shard1));

        shardManager.startShards(clusterConfig);

        Set<String> localShards = shardManager.localShardIds();
        assertEquals(1, localShards.size());
        assertTrue(localShards.contains("shard-0"));
        assertNotNull(shardManager.getShard("shard-0"));
        assertNotNull(shardManager.getStateMachine("shard-0"));
    }

    @Test
    @DisplayName("Should NOT start shard when node does not participate")
    void shouldNotStartShardWhenNodeDoesNotParticipate() {
        ShardConfig shard1 = new ShardConfig(
                "shard-0",
                7100,
                List.of("node-2:localhost:7100", "node-3:localhost:7100", "node-4:localhost:7100")
        );

        ClusterConfig clusterConfig = new ClusterConfig(List.of(shard1));

        shardManager.startShards(clusterConfig);

        Set<String> localShards = shardManager.localShardIds();
        assertTrue(localShards.isEmpty(), "Should not start shard when node is not a peer");
        assertNull(shardManager.getShard("shard-0"));
        assertNull(shardManager.getStateMachine("shard-0"));
    }

    @Test
    @DisplayName("Should throw exception when node participates in multiple shards")
    void shouldThrowExceptionWhenNodeParticipatesInMultipleShards() {
        ShardConfig shard1 = new ShardConfig(
                "shard-0",
                7100,
                List.of("node-1:localhost:7100", "node-2:localhost:7100", "node-3:localhost:7100")
        );
        ShardConfig shard2 = new ShardConfig(
                "shard-1",
                7101,
                List.of("node-1:localhost:7101", "node-4:localhost:7101", "node-5:localhost:7101")  // node-1 in both!
        );

        ClusterConfig clusterConfig = new ClusterConfig(List.of(shard1, shard2));

        IllegalStateException exception = assertThrows(
            IllegalStateException.class,
            () -> shardManager.startShards(clusterConfig),
            "Should throw IllegalStateException when node participates in multiple shards"
        );

        assertTrue(exception.getMessage().contains("cannot participate in multiple shards"));
        assertTrue(exception.getMessage().contains("node-1"));
    }

    @Test
    @DisplayName("Should start only the shard where node participates (classic architecture)")
    void shouldStartOnlyTheShardWhereNodeParticipates() {
        // Classic architecture: different nodes for different shards
        ShardConfig shard1 = new ShardConfig(
                "shard-0",
                7100,
                List.of("node-1:localhost:7100", "node-2:localhost:7100", "node-3:localhost:7100")
        );
        ShardConfig shard2 = new ShardConfig(
                "shard-1",
                7101,
                List.of("node-4:localhost:7101", "node-5:localhost:7101", "node-6:localhost:7101")  // Different nodes
        );
        ShardConfig shard3 = new ShardConfig(
                "shard-2",
                7102,
                List.of("node-7:localhost:7102", "node-8:localhost:7102", "node-9:localhost:7102")  // Different nodes
        );

        ClusterConfig clusterConfig = new ClusterConfig(List.of(shard1, shard2, shard3));

        shardManager.startShards(clusterConfig);

        // node-1 should only participate in shard-0
        Set<String> localShards = shardManager.localShardIds();
        assertEquals(1, localShards.size());
        assertTrue(localShards.contains("shard-0"));
        assertFalse(localShards.contains("shard-1"));
        assertFalse(localShards.contains("shard-2"));
    }

    @Test
    @DisplayName("Should select shard for key using consistent hashing")
    void shouldSelectShardForKeyUsingConsistentHashing() {
        // Use only one shard for this test (node-1 participates)
        ShardConfig shard1 = new ShardConfig("shard-0", 7100, List.of("node-1:localhost:7100", "node-2:localhost:7100"));

        ClusterConfig clusterConfig = new ClusterConfig(List.of(shard1));
        shardManager.startShards(clusterConfig);

        String selectedShard = shardManager.selectShardForKey("some-key");
        assertNotNull(selectedShard);
        assertEquals("shard-0", selectedShard);

        // Same key should always map to same shard
        String selectedShard2 = shardManager.selectShardForKey("some-key");
        assertEquals(selectedShard, selectedShard2);
    }

    @Test
    @DisplayName("Should return null when selecting shard with null key")
    void shouldReturnNullWhenSelectingShardWithNullKey() {
        ShardConfig shard1 = new ShardConfig("shard-0", 7100, List.of("node-1:localhost:7100", "node-2:localhost:7100"));
        ClusterConfig clusterConfig = new ClusterConfig(List.of(shard1));
        shardManager.startShards(clusterConfig);

        String selectedShard = shardManager.selectShardForKey(null);
        assertNull(selectedShard);
    }

    @Test
    @DisplayName("Should stop all shards without errors")
    void shouldStopAllShardsWithoutErrors() {
        // Use only one shard
        ShardConfig shard1 = new ShardConfig("shard-0", 7100, List.of("node-1:localhost:7100", "node-2:localhost:7100"));

        ClusterConfig clusterConfig = new ClusterConfig(List.of(shard1));
        shardManager.startShards(clusterConfig);

        // Should not throw
        assertDoesNotThrow(() -> shardManager.stopAll());
    }

    @Test
    @DisplayName("Should return null for non-existent shard")
    void shouldReturnNullForNonExistentShard() {
        assertNull(shardManager.getShard("non-existent"));
        assertNull(shardManager.getStateMachine("non-existent"));
    }

    @Test
    @DisplayName("Should handle empty cluster config gracefully")
    void shouldHandleEmptyClusterConfigGracefully() {
        ClusterConfig emptyConfig = new ClusterConfig(List.of());

        assertDoesNotThrow(() -> shardManager.startShards(emptyConfig));

        Set<String> localShards = shardManager.localShardIds();
        assertTrue(localShards.isEmpty());
    }

    @Test
    @DisplayName("Should return unmodifiable set of local shard IDs")
    void shouldReturnUnmodifiableSetOfLocalShardIds() {
        ShardConfig shard1 = new ShardConfig("shard-0", 7100, List.of("node-1:localhost:7100", "node-2:localhost:7100"));
        ClusterConfig clusterConfig = new ClusterConfig(List.of(shard1));
        shardManager.startShards(clusterConfig);

        Set<String> localShards = shardManager.localShardIds();

        // Should throw when trying to modify
        assertThrows(UnsupportedOperationException.class, () -> localShards.add("new-shard"));
    }

    @Test
    @DisplayName("Should NOT start shard with single node (single-node Raft not supported)")
    void shouldNotStartShardWithSingleNode() {
        ShardConfig shard1 = new ShardConfig(
                "shard-0",
                7100,
                List.of("node-1:localhost:7100")  // Only this node - no other peers
        );

        ClusterConfig clusterConfig = new ClusterConfig(List.of(shard1));
        shardManager.startShards(clusterConfig);

        // Single-node Raft is not supported, shard should not start
        Set<String> localShards = shardManager.localShardIds();
        assertTrue(localShards.isEmpty(), "Single-node shard should not start");
        assertNull(shardManager.getShard("shard-0"));
        assertNull(shardManager.getStateMachine("shard-0"));
    }
}