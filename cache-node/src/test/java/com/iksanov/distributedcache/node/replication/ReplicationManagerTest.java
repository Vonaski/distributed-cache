package com.iksanov.distributedcache.node.replication;

import com.iksanov.distributedcache.common.cluster.NodeInfo;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import java.util.function.Function;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for ReplicationManager.
 * <p>
 * Covers:
 *  - replication triggered only if current node is master
 *  - proper delegate calls to sender and receiver
 *  - error handling in onReplicationReceived
 *  - shutdown propagation
 */
public class ReplicationManagerTest {

    private NodeInfo currentNode;
    private NodeInfo replicaNode;

    private ReplicationSender sender;
    private ReplicationReceiver receiver;
    private Function<String, NodeInfo> resolver;

    private ReplicationManager manager;

    @BeforeEach
    void setUp() {
        currentNode = new NodeInfo("node-A", "localhost", 9001);
        NodeInfo masterNode = currentNode;
        replicaNode = new NodeInfo("node-B", "localhost", 9002);

        sender = mock(ReplicationSender.class);
        receiver = mock(ReplicationReceiver.class);
        resolver = mock(Function.class);

        manager = new ReplicationManager(currentNode, sender, receiver, resolver);
    }

    @Test
    void onLocalSet_shouldTriggerReplication_whenCurrentIsMaster() {
        String key = "user:1";
        String value = "Alice";

        when(resolver.apply(key)).thenReturn(currentNode);

        manager.onLocalSet(key, value);

        ArgumentCaptor<ReplicationTask> captor = ArgumentCaptor.forClass(ReplicationTask.class);
        verify(sender, times(1)).replicate(eq(currentNode), captor.capture());

        ReplicationTask task = captor.getValue();
        assertEquals(key, task.key());
        assertEquals(value, task.value());
        assertEquals(ReplicationTask.Operation.SET, task.operation());
        assertEquals(currentNode.nodeId(), task.origin());
    }

    @Test
    void onLocalSet_shouldSkipReplication_whenNotMaster() {
        String key = "user:2";
        String value = "Bob";

        when(resolver.apply(key)).thenReturn(replicaNode);

        manager.onLocalSet(key, value);

        verify(sender, never()).replicate(any(), any());
    }

    @Test
    void onLocalDelete_shouldTriggerReplication_whenCurrentIsMaster() {
        String key = "product:1";

        when(resolver.apply(key)).thenReturn(currentNode);

        manager.onLocalDelete(key);

        ArgumentCaptor<ReplicationTask> captor = ArgumentCaptor.forClass(ReplicationTask.class);
        verify(sender, times(1)).replicate(eq(currentNode), captor.capture());

        ReplicationTask task = captor.getValue();
        assertEquals(ReplicationTask.Operation.DELETE, task.operation());
        assertEquals(currentNode.nodeId(), task.origin());
        assertEquals(key, task.key());
    }

    @Test
    void onLocalDelete_shouldSkipReplication_whenNotMaster() {
        String key = "product:2";
        when(resolver.apply(key)).thenReturn(replicaNode);

        manager.onLocalDelete(key);

        verify(sender, never()).replicate(any(), any());
    }

    @Test
    void onReplicationReceived_shouldDelegateToReceiver() {
        ReplicationTask task = ReplicationTask.ofSet("key", "value", "node-B");

        manager.onReplicationReceived(task);

        verify(receiver, times(1)).applyTask(task);
    }

    @Test
    void onReplicationReceived_shouldCatchExceptions() {
        ReplicationTask task = ReplicationTask.ofSet("key", "value", "node-B");
        doThrow(new RuntimeException("boom")).when(receiver).applyTask(any());

        assertDoesNotThrow(() -> manager.onReplicationReceived(task));

        verify(receiver, times(1)).applyTask(task);
    }

    @Test
    void shutdown_shouldCloseSenderGracefully() {
        manager.shutdown();
        verify(sender, times(1)).shutdown();
    }

    @Test
    void onLocalSet_shouldSkipWhenResolverReturnsNull() {
        when(resolver.apply("ghost")).thenReturn(null);
        manager.onLocalSet("ghost", "v");
        verify(sender, never()).replicate(any(), any());
    }

    @Test
    void onLocalSet_shouldThrowOnNullKey() {
        assertThrows(NullPointerException.class, () -> manager.onLocalSet(null, "v"));
    }

}
