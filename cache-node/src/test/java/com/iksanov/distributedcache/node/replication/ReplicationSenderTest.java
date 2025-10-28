package com.iksanov.distributedcache.node.replication;

import com.iksanov.distributedcache.common.cluster.NodeInfo;
import com.iksanov.distributedcache.common.cluster.ReplicaManager;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import org.junit.jupiter.api.*;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Final unit tests for ReplicationSender.
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
        // use production-style sender (no spy)
        sender = new ReplicationSender(replicaManager);
        master = new NodeInfo("node-A", "127.0.0.1", 9001);
        replicaA = new NodeInfo("node-B", "127.0.0.1", 9002);
        replicaB = new NodeInfo("node-C", "127.0.0.1", 9003);
    }

    @AfterEach
    void tearDown() {
        sender.shutdown();
    }

    @Test
    @DisplayName("should skip null and self replicas without throwing")
    void shouldSkipNullAndSelfReplicas() {
        Set<NodeInfo> replicas = new HashSet<>();
        replicas.add(null);
        replicas.add(master);
        replicas.add(replicaA);

        when(replicaManager.getReplicas(master)).thenReturn(replicas);

        assertDoesNotThrow(() -> sender.replicate(master, ReplicationTask.ofSet("k","v", master.nodeId())));
    }

    @Test
    @DisplayName("should handle empty replica set gracefully")
    void shouldHandleEmptyReplicaSet() {
        when(replicaManager.getReplicas(master)).thenReturn(Set.of());
        assertDoesNotThrow(() -> sender.replicate(master, ReplicationTask.ofSet("k","v", master.nodeId())));
    }

    @Test
    @DisplayName("should reuse existing channel and call writeAndFlush")
    void shouldReuseExistingChannel() {
        when(replicaManager.getReplicas(master)).thenReturn(Set.of(replicaA));

        Channel mockChannel = mock(Channel.class);
        when(mockChannel.isActive()).thenReturn(true);

        ChannelFuture successFuture = mock(ChannelFuture.class);
        when(mockChannel.writeAndFlush(any())).thenReturn(successFuture);
        // make addListener call safe (no NPE) if code attaches listener on mock future
        doAnswer(invocation -> null).when(successFuture).addListener(any());

        sender.channelsMap().put(replicaA.nodeId(), mockChannel);

        sender.replicate(master, ReplicationTask.ofSet("x","1", master.nodeId()));
        sender.replicate(master, ReplicationTask.ofSet("y","2", master.nodeId()));

        verify(mockChannel, atLeast(2)).writeAndFlush(any());
    }

    @Test
    @DisplayName("should not crash on write failure")
    void shouldHandleWriteFailure() {
        when(replicaManager.getReplicas(master)).thenReturn(Set.of(replicaA));

        Channel mockChannel = mock(Channel.class);
        when(mockChannel.isActive()).thenReturn(true);

        ChannelFuture failFuture = mock(ChannelFuture.class);
        when(failFuture.isSuccess()).thenReturn(false);
        when(mockChannel.writeAndFlush(any())).thenReturn(failFuture);
        doAnswer(invocation -> null).when(failFuture).addListener(any());

        sender.channelsMap().put(replicaA.nodeId(), mockChannel);

        assertDoesNotThrow(() -> sender.replicate(master, ReplicationTask.ofSet("k","v", master.nodeId())));
    }

    @Test
    @DisplayName("should cleanup channels on shutdown (primary check)")
    void shouldCleanupChannelsOnShutdown() {
        Channel ch = mock(Channel.class);
        when(ch.isOpen()).thenReturn(true);
        // make close return a mock future so syncUninterruptibly is safe
        ChannelFuture closeFuture = mock(ChannelFuture.class);
        when(ch.close()).thenReturn(closeFuture);
        doAnswer(invocation -> null).when(closeFuture).syncUninterruptibly();

        sender.channelsMap().put(replicaA.nodeId(), ch);
        sender.channelsMap().put(replicaB.nodeId(), ch);

        assertDoesNotThrow(() -> sender.shutdown());

        assertTrue(sender.channelsMap().isEmpty(), "Channels map must be cleared");
    }
}
