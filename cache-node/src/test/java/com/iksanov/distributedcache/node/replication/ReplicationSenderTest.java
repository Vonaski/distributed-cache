package com.iksanov.distributedcache.node.replication;

import com.iksanov.distributedcache.common.cluster.NodeInfo;
import com.iksanov.distributedcache.common.cluster.ReplicaManager;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import org.junit.jupiter.api.*;

import java.util.List;
import java.util.Set;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link ReplicationSender}.
 * <p>
 * Covers:
 *  - Replica filtering (null, self, empty sets)
 *  - Channel reuse for active connections
 *  - Write failure handling
 *  - Graceful shutdown with channel cleanup
 *  - Multiple replicas handling
 */
class ReplicationSenderTest {

    private ReplicaManager replicaManager;
    private ReplicationSender sender;
    private NodeInfo master;
    private NodeInfo replicaA;
    private NodeInfo replicaB;

    @BeforeEach
    void setUp() {
        replicaManager = mock(ReplicaManager.class);
        sender = new ReplicationSender(replicaManager, null);
        master = new NodeInfo("node-A", "127.0.0.1", 9001);
        replicaA = new NodeInfo("node-B", "127.0.0.1", 9002);
        replicaB = new NodeInfo("node-C", "127.0.0.1", 9003);
    }

    @AfterEach
    void tearDown() {
        if (sender != null) sender.shutdown();
    }

    @Test
    @DisplayName("replicate() should skip null replicas without throwing")
    void shouldSkipNullReplicas() {
        Set<NodeInfo> replicas = new java.util.HashSet<>();
        replicas.add(replicaA);
        replicas.add(null);
        when(replicaManager.getReplicas(master)).thenReturn(replicas);
        assertDoesNotThrow(() -> sender.replicate(master, ReplicationTask.ofSet("k", "v", master.nodeId(), 0L)));
    }

    @Test
    @DisplayName("replicate() should skip self (master == replica) without throwing")
    void shouldSkipSelfReplicas() {
        when(replicaManager.getReplicas(master)).thenReturn(Set.of(master, replicaA));
        assertDoesNotThrow(() -> sender.replicate(master, ReplicationTask.ofSet("k", "v", master.nodeId(), 0L)));
    }

    @Test
    @DisplayName("replicate() should handle empty replica set gracefully")
    void shouldHandleEmptyReplicaSet() {
        when(replicaManager.getReplicas(master)).thenReturn(Set.of());
        assertDoesNotThrow(() -> sender.replicate(master, ReplicationTask.ofSet("k", "v", master.nodeId(), 0L)));
    }

    @Test
    @DisplayName("replicate() should handle null replica set from manager")
    void shouldHandleNullReplicaSet() {
        when(replicaManager.getReplicas(master)).thenReturn(null);
        assertDoesNotThrow(() -> sender.replicate(master, ReplicationTask.ofSet("k", "v", master.nodeId(), 0L)));
    }

    @Test
    @DisplayName("replicate() should reuse existing active channel")
    void shouldReuseExistingChannel() {
        when(replicaManager.getReplicas(master)).thenReturn(Set.of(replicaA));
        Channel mockChannel = mock(Channel.class);
        when(mockChannel.isActive()).thenReturn(true);
        ChannelFuture writeFuture = mock(ChannelFuture.class);
        when(mockChannel.writeAndFlush(any())).thenReturn(writeFuture);
        doAnswer(inv -> null).when(writeFuture).addListener(any());
        sender.channelsMap().put(replicaA.nodeId(), mockChannel);
        sender.replicate(master, ReplicationTask.ofSet("k1", "v1", master.nodeId(), 0L));
        sender.replicate(master, ReplicationTask.ofSet("k2", "v2", master.nodeId(), 0L));
        verify(mockChannel, times(2)).writeAndFlush(any(ReplicationTask.class));
    }

    @Test
    @DisplayName("replicate() should remove channel on write failure")
    void shouldRemoveChannelOnWriteFailure() {
        when(replicaManager.getReplicas(master)).thenReturn(Set.of(replicaA));
        Channel mockChannel = mock(Channel.class);
        when(mockChannel.isActive()).thenReturn(true);
        ChannelFuture failFuture = mock(ChannelFuture.class);
        when(failFuture.isSuccess()).thenReturn(false);
        when(mockChannel.writeAndFlush(any())).thenReturn(failFuture);
        doAnswer(inv -> {
            var listener = inv.getArgument(0, io.netty.channel.ChannelFutureListener.class);
            listener.operationComplete(failFuture);
            return null;
        }).when(failFuture).addListener(any());
        sender.channelsMap().put(replicaA.nodeId(), mockChannel);
        sender.replicate(master, ReplicationTask.ofSet("k", "v", master.nodeId(), 0L));
        assertFalse(sender.channelsMap().containsKey(replicaA.nodeId()), "Channel should be removed after write failure");
    }

    @Test
    @DisplayName("replicate() should handle inactive channel by reconnecting")
    void shouldReconnectWhenChannelInactive() {
        when(replicaManager.getReplicas(master)).thenReturn(Set.of(replicaA));
        Channel inactiveChannel = mock(Channel.class);
        when(inactiveChannel.isActive()).thenReturn(false);
        sender.channelsMap().put(replicaA.nodeId(), inactiveChannel);
        assertDoesNotThrow(() -> sender.replicate(master, ReplicationTask.ofSet("k", "v", master.nodeId(), 0L)));
    }

    @Test
    @DisplayName("replicate() should handle multiple replicas")
    void shouldHandleMultipleReplicas() {
        when(replicaManager.getReplicas(master)).thenReturn(Set.of(replicaA, replicaB));
        Channel channelA = mock(Channel.class);
        when(channelA.isActive()).thenReturn(true);
        ChannelFuture futureA = mock(ChannelFuture.class);
        when(channelA.writeAndFlush(any())).thenReturn(futureA);
        doAnswer(inv -> null).when(futureA).addListener(any());
        Channel channelB = mock(Channel.class);
        when(channelB.isActive()).thenReturn(true);
        ChannelFuture futureB = mock(ChannelFuture.class);
        when(channelB.writeAndFlush(any())).thenReturn(futureB);
        doAnswer(inv -> null).when(futureB).addListener(any());
        sender.channelsMap().put(replicaA.nodeId(), channelA);
        sender.channelsMap().put(replicaB.nodeId(), channelB);
        sender.replicate(master, ReplicationTask.ofSet("k", "v", master.nodeId(), 0L));
        verify(channelA, times(1)).writeAndFlush(any(ReplicationTask.class));
        verify(channelB, times(1)).writeAndFlush(any(ReplicationTask.class));
    }

    @Test
    @DisplayName("shutdown() should close all channels and clear map")
    void shouldCloseAllChannelsOnShutdown() {
        ChannelFuture closeFuture1 = mock(ChannelFuture.class);
        doAnswer(inv -> null).when(closeFuture1).syncUninterruptibly();
        Channel ch1 = mock(Channel.class);
        when(ch1.isOpen()).thenReturn(true);
        when(ch1.close()).thenReturn(closeFuture1);
        ChannelFuture closeFuture2 = mock(ChannelFuture.class);
        doAnswer(inv -> null).when(closeFuture2).syncUninterruptibly();
        Channel ch2 = mock(Channel.class);
        when(ch2.isOpen()).thenReturn(true);
        when(ch2.close()).thenReturn(closeFuture2);
        sender.channelsMap().put(replicaA.nodeId(), ch1);
        sender.channelsMap().put(replicaB.nodeId(), ch2);
        sender.shutdown();
        verify(ch1, times(1)).close();
        verify(ch2, times(1)).close();
        assertTrue(sender.channelsMap().isEmpty(), "Channels map must be cleared after shutdown");
    }

    @Test
    @DisplayName("shutdown() should handle channels that throw on close")
    void shouldHandleExceptionsOnShutdown() {
        Channel badChannel = mock(Channel.class);
        when(badChannel.isOpen()).thenReturn(true);
        ChannelFuture badFuture = mock(ChannelFuture.class);
        doThrow(new RuntimeException("close failed")).when(badFuture).syncUninterruptibly();
        when(badChannel.close()).thenReturn(badFuture);
        sender.channelsMap().put(replicaA.nodeId(), badChannel);
        assertDoesNotThrow(() -> sender.shutdown(), "Shutdown must not throw even if channel.close() fails");
        assertTrue(sender.channelsMap().isEmpty(), "Channels map must be cleared even after exceptions");
    }

    @Test
    @DisplayName("shutdown() should be idempotent")
    void shouldBeIdempotent() {
        assertDoesNotThrow(() -> {
            sender.shutdown();
            sender.shutdown();
        }, "Multiple shutdown calls should not throw");
    }

    @Test
    @DisplayName("sendHeartbeatToReplicas() should send HEARTBEAT to all replicas")
    void shouldSendHeartbeatToAllReplicas() {
        when(replicaManager.getReplicas(master)).thenReturn(Set.of(replicaA, replicaB));
        Channel channelA = mock(Channel.class);
        when(channelA.isActive()).thenReturn(true);
        ChannelFuture futureA = mock(ChannelFuture.class);
        when(channelA.writeAndFlush(any())).thenReturn(futureA);
        doAnswer(inv -> null).when(futureA).addListener(any());
        Channel channelB = mock(Channel.class);
        when(channelB.isActive()).thenReturn(true);
        ChannelFuture futureB = mock(ChannelFuture.class);
        when(channelB.writeAndFlush(any())).thenReturn(futureB);
        doAnswer(inv -> null).when(futureB).addListener(any());
        sender.channelsMap().put(replicaA.nodeId(), channelA);
        sender.channelsMap().put(replicaB.nodeId(), channelB);
        sender.sendHeartbeatToReplicas(master);
        verify(channelA, times(1)).writeAndFlush(any(ReplicationTask.class));
        verify(channelB, times(1)).writeAndFlush(any(ReplicationTask.class));
    }

    @Test
    @DisplayName("sendHeartbeatToReplicas() should skip null replicas")
    void shouldSkipNullReplicasInHeartbeat() {
        Set<NodeInfo> replicas = new java.util.HashSet<>();
        replicas.add(replicaA);
        replicas.add(null);
        when(replicaManager.getReplicas(master)).thenReturn(replicas);
        assertDoesNotThrow(() -> sender.sendHeartbeatToReplicas(master));
    }

    @Test
    @DisplayName("sendHeartbeatToReplicas() should skip self in replicas")
    void shouldSkipSelfInHeartbeat() {
        when(replicaManager.getReplicas(master)).thenReturn(Set.of(master, replicaA));
        Channel channelA = mock(Channel.class);
        when(channelA.isActive()).thenReturn(true);
        ChannelFuture futureA = mock(ChannelFuture.class);
        when(channelA.writeAndFlush(any())).thenReturn(futureA);
        doAnswer(inv -> null).when(futureA).addListener(any());
        sender.channelsMap().put(replicaA.nodeId(), channelA);
        sender.sendHeartbeatToReplicas(master);
        verify(channelA, times(1)).writeAndFlush(any(ReplicationTask.class));
    }

    @Test
    @DisplayName("sendHeartbeatToReplicas() should handle empty replica set")
    void shouldHandleEmptyReplicaSetInHeartbeat() {
        when(replicaManager.getReplicas(master)).thenReturn(Set.of());
        assertDoesNotThrow(() -> sender.sendHeartbeatToReplicas(master));
    }

    @Test
    @DisplayName("sendHeartbeatToReplicas() should handle null replica set")
    void shouldHandleNullReplicaSetInHeartbeat() {
        when(replicaManager.getReplicas(master)).thenReturn(null);
        assertDoesNotThrow(() -> sender.sendHeartbeatToReplicas(master));
    }

    @Test
    @DisplayName("sendHeartbeatToReplicas() should establish connection if channel doesn't exist")
    void shouldEstablishConnectionForHeartbeat() {
        when(replicaManager.getReplicas(master)).thenReturn(Set.of(replicaA));
        assertTrue(sender.channelsMap().isEmpty());
        sender.sendHeartbeatToReplicas(master);
    }

    @Test
    @DisplayName("sendHeartbeatToReplicas() should handle inactive channel")
    void shouldHandleInactiveChannelInHeartbeat() {
        when(replicaManager.getReplicas(master)).thenReturn(Set.of(replicaA));
        Channel inactiveChannel = mock(Channel.class);
        when(inactiveChannel.isActive()).thenReturn(false);
        sender.channelsMap().put(replicaA.nodeId(), inactiveChannel);
        assertDoesNotThrow(() -> sender.sendHeartbeatToReplicas(master));
    }

    @Test
    @DisplayName("sendHeartbeatToReplicas() should not throw on write failure")
    void shouldNotThrowOnHeartbeatWriteFailure() {
        when(replicaManager.getReplicas(master)).thenReturn(Set.of(replicaA));
        Channel mockChannel = mock(Channel.class);
        when(mockChannel.isActive()).thenReturn(true);
        ChannelFuture failFuture = mock(ChannelFuture.class);
        when(failFuture.isSuccess()).thenReturn(false);
        when(failFuture.cause()).thenReturn(new RuntimeException("Write failed"));
        when(mockChannel.writeAndFlush(any())).thenReturn(failFuture);
        doAnswer(inv -> failFuture).when(failFuture).addListener(any());
        sender.channelsMap().put(replicaA.nodeId(), mockChannel);
        assertDoesNotThrow(() -> sender.sendHeartbeatToReplicas(master));
        verify(mockChannel, times(1)).writeAndFlush(any(ReplicationTask.class));
    }

    @Test
    @DisplayName("getChannelToNode() should return existing channel")
    void shouldReturnExistingChannel() {
        Channel mockChannel = mock(Channel.class);
        sender.channelsMap().put(replicaA.nodeId(), mockChannel);
        Channel result = sender.getChannelToNode(replicaA);
        assertEquals(mockChannel, result);
    }

    @Test
    @DisplayName("getChannelToNode() should return null if no channel exists")
    void shouldReturnNullIfNoChannelExists() {
        Channel result = sender.getChannelToNode(replicaA);
        assertNull(result);
    }

    @Test
    @DisplayName("getChannelToNode() should return null for null node")
    void shouldReturnNullForNullNode() {
        assertThrows(NullPointerException.class, () -> sender.getChannelToNode(null));
    }

    @Test
    @DisplayName("updateReplicaTargets() should register all replicas for new master")
    void shouldRegisterAllReplicasForNewMaster() {
        NodeInfo newMaster = new NodeInfo("replica-1-promoted", "localhost", 7003, 7103);
        NodeInfo replica2 = new NodeInfo("replica-2-master-1", "localhost", 7004, 7104);
        NodeInfo replica3 = new NodeInfo("replica-3-master-1", "localhost", 7005, 7105);
        java.util.List<NodeInfo> replicas = java.util.Arrays.asList(replica2, replica3);
        sender.updateReplicaTargets(newMaster, replicas);
        verify(replicaManager).registerReplica(newMaster, replica2);
        verify(replicaManager).registerReplica(newMaster, replica3);
        verify(replicaManager, times(1)).getReplicas(newMaster);
    }

    @Test
    @DisplayName("updateReplicaTargets() should handle empty replica list")
    void shouldHandleEmptyReplicaListInUpdate() {
        NodeInfo newMaster = new NodeInfo("replica-1-promoted", "localhost", 7003, 7103);
        assertDoesNotThrow(() -> sender.updateReplicaTargets(newMaster, java.util.Collections.emptyList()));
        verify(replicaManager, never()).registerReplica(any(), any());
    }

    @Test
    @DisplayName("updateReplicaTargets() should handle null replica list")
    void shouldHandleNullReplicaListInUpdate() {
        NodeInfo newMaster = new NodeInfo("replica-1-promoted", "localhost", 7003, 7103);
        assertDoesNotThrow(() -> sender.updateReplicaTargets(newMaster, null));
        verify(replicaManager, never()).registerReplica(any(), any());
    }

    @Test
    @DisplayName("updateReplicaTargets() should handle null newMaster gracefully")
    void shouldHandleNullNewMaster() {
        NodeInfo replica2 = new NodeInfo("replica-2", "localhost", 7004, 7104);
        assertDoesNotThrow(() -> sender.updateReplicaTargets(null, List.of(replica2)));
        verify(replicaManager, never()).registerReplica(any(), any());
    }

    @Test
    @DisplayName("updateReplicaTargets() should continue registering after one failure")
    void shouldContinueRegisteringAfterFailure() {
        NodeInfo newMaster = new NodeInfo("replica-1-promoted", "localhost", 7003, 7103);
        NodeInfo replica2 = new NodeInfo("replica-2", "localhost", 7004, 7104);
        NodeInfo replica3 = new NodeInfo("replica-3", "localhost", 7005, 7105);
        java.util.List<NodeInfo> replicas = java.util.Arrays.asList(replica2, replica3);
        doThrow(new RuntimeException("Registration failed")).when(replicaManager).registerReplica(newMaster, replica2);
        assertDoesNotThrow(() -> sender.updateReplicaTargets(newMaster, replicas));
        verify(replicaManager).registerReplica(newMaster, replica2);
        verify(replicaManager).registerReplica(newMaster, replica3);
    }

    @Test
    @DisplayName("updateReplicaTargets() should verify registration by calling getReplicas")
    void shouldVerifyRegistrationByCallingGetReplicas() {
        NodeInfo newMaster = new NodeInfo("replica-1-promoted", "localhost", 7003, 7103);
        NodeInfo replica2 = new NodeInfo("replica-2", "localhost", 7004, 7104);
        when(replicaManager.getReplicas(newMaster)).thenReturn(Set.of(replica2));
        sender.updateReplicaTargets(newMaster, List.of(replica2));
        verify(replicaManager).getReplicas(newMaster);
    }

    @Test
    @DisplayName("updateReplicaTargets() should handle single replica")
    void shouldHandleSingleReplica() {
        NodeInfo newMaster = new NodeInfo("replica-1-promoted", "localhost", 7003, 7103);
        NodeInfo replica2 = new NodeInfo("replica-2", "localhost", 7004, 7104);
        sender.updateReplicaTargets(newMaster, List.of(replica2));
        verify(replicaManager, times(1)).registerReplica(newMaster, replica2);
    }

    @Test
    @DisplayName("updateReplicaTargets() should handle multiple replicas")
    void shouldHandleMultipleReplicasInUpdate() {
        NodeInfo newMaster = new NodeInfo("replica-1-promoted", "localhost", 7003, 7103);
        NodeInfo replica2 = new NodeInfo("replica-2", "localhost", 7004, 7104);
        NodeInfo replica3 = new NodeInfo("replica-3", "localhost", 7005, 7105);
        NodeInfo replica4 = new NodeInfo("replica-4", "localhost", 7006, 7106);
        java.util.List<NodeInfo> replicas = java.util.Arrays.asList(replica2, replica3, replica4);
        sender.updateReplicaTargets(newMaster, replicas);
        verify(replicaManager).registerReplica(newMaster, replica2);
        verify(replicaManager).registerReplica(newMaster, replica3);
        verify(replicaManager).registerReplica(newMaster, replica4);
        verify(replicaManager, times(1)).getReplicas(newMaster);
    }
}