package com.iksanov.distributedcache.node.replication;

import com.iksanov.distributedcache.common.cluster.NodeInfo;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link ReplicationManager}.
 * <p>
 * Tests async replication behavior with proper synchronization.
 * Simplified for master-replica architecture where sharding is handled by proxy.
 */
@DisplayName("ReplicationManager Tests")
public class ReplicationManagerTest {

    private static final int ASYNC_TIMEOUT_MS = 1000;

    private NodeInfo currentNode;
    private NodeInfo replicaNode;
    private ReplicationSender sender;
    private ReplicationReceiver receiver;
    private ReplicationManager manager;

    @BeforeEach
    void setUp() {
        currentNode = new NodeInfo("node-A", "localhost", 9001);
        replicaNode = new NodeInfo("node-B", "localhost", 9002);
        sender = mock(ReplicationSender.class);
        receiver = mock(ReplicationReceiver.class);
        manager = new ReplicationManager(currentNode, sender, receiver, null);
    }

    @AfterEach
    void tearDown() {
        if (manager != null) {
            manager.shutdown();
        }
    }

    @Test
    @DisplayName("Should asynchronously replicate SET")
    void onLocalSet_shouldAsyncReplicate() throws InterruptedException {
        String key = "user:1";
        String value = "Alice";

        CountDownLatch latch = new CountDownLatch(1);
        doAnswer(invocation -> {
            latch.countDown();
            return null;
        }).when(sender).replicate(any(), any());

        manager.onLocalSet(key, value);
        assertTrue(latch.await(ASYNC_TIMEOUT_MS, TimeUnit.MILLISECONDS), "Replication should complete within timeout");

        ArgumentCaptor<ReplicationTask> taskCaptor = ArgumentCaptor.forClass(ReplicationTask.class);
        verify(sender, timeout(ASYNC_TIMEOUT_MS)).replicate(eq(currentNode), taskCaptor.capture());

        ReplicationTask capturedTask = taskCaptor.getValue();
        assertAll("Task properties",
                () -> assertEquals(key, capturedTask.key()),
                () -> assertEquals(value, capturedTask.value()),
                () -> assertEquals(ReplicationTask.Operation.SET, capturedTask.operation()),
                () -> assertEquals(currentNode.nodeId(), capturedTask.origin())
        );
    }

    @Test
    @DisplayName("Should asynchronously replicate DELETE")
    void onLocalDelete_shouldAsyncReplicate() throws InterruptedException {
        String key = "product:1";

        CountDownLatch latch = new CountDownLatch(1);
        doAnswer(invocation -> {
            latch.countDown();
            return null;
        }).when(sender).replicate(any(), any());

        manager.onLocalDelete(key);
        assertTrue(latch.await(ASYNC_TIMEOUT_MS, TimeUnit.MILLISECONDS));

        ArgumentCaptor<ReplicationTask> taskCaptor = ArgumentCaptor.forClass(ReplicationTask.class);
        verify(sender, timeout(ASYNC_TIMEOUT_MS)).replicate(eq(currentNode), taskCaptor.capture());

        ReplicationTask capturedTask = taskCaptor.getValue();
        assertAll("Delete task properties",
                () -> assertEquals(key, capturedTask.key()),
                () -> assertNull(capturedTask.value()),
                () -> assertEquals(ReplicationTask.Operation.DELETE, capturedTask.operation()),
                () -> assertEquals(currentNode.nodeId(), capturedTask.origin())
        );
    }

    @Test
    @DisplayName("Should handle replication errors gracefully")
    void onLocalSet_shouldHandleReplicationError() throws InterruptedException {
        String key = "error-key";
        String value = "error-value";

        CountDownLatch latch = new CountDownLatch(1);
        doAnswer(invocation -> {
            latch.countDown();
            throw new RuntimeException("Network error");
        }).when(sender).replicate(any(), any());

        manager.onLocalSet(key, value);

        assertTrue(latch.await(ASYNC_TIMEOUT_MS, TimeUnit.MILLISECONDS));
        verify(sender, timeout(ASYNC_TIMEOUT_MS)).replicate(any(), any());
    }

    @Test
    @DisplayName("Should handle multiple concurrent replication requests")
    void shouldHandleConcurrentReplications() throws InterruptedException {
        int operationCount = 10;

        CountDownLatch latch = new CountDownLatch(operationCount);
        doAnswer(invocation -> {
            latch.countDown();
            return null;
        }).when(sender).replicate(any(), any());

        for (int i = 0; i < operationCount; i++) {
            manager.onLocalSet("key-" + i, "value-" + i);
        }

        assertTrue(latch.await(ASYNC_TIMEOUT_MS * 2, TimeUnit.MILLISECONDS),
                "All replications should complete");
        verify(sender, timeout(ASYNC_TIMEOUT_MS * 2).times(operationCount))
                .replicate(eq(currentNode), any());
    }

    @Test
    @DisplayName("Should synchronously apply received replication task")
    void onReplicationReceived_shouldApplyTask() {
        ReplicationTask task = ReplicationTask.ofSet("remote-key", "remote-value", "node-B");
        manager.onReplicationReceived(task);
        verify(receiver).applyTask(task);
    }

    @Test
    @DisplayName("Should handle receiver errors without propagating")
    void onReplicationReceived_shouldHandleErrors() {
        ReplicationTask task = ReplicationTask.ofSet("key", "value", "node-B");
        doThrow(new RuntimeException("Storage error")).when(receiver).applyTask(any());
        assertDoesNotThrow(() -> manager.onReplicationReceived(task));
        verify(receiver).applyTask(task);
    }

    @Test
    @DisplayName("Should validate null parameters")
    void shouldValidateNullParameters() {
        assertAll("Null validation",
                () -> assertThrows(NullPointerException.class,
                        () -> manager.onLocalSet(null, "value")),
                () -> assertThrows(NullPointerException.class,
                        () -> manager.onLocalSet("key", null)),
                () -> assertThrows(NullPointerException.class,
                        () -> manager.onLocalDelete(null))
        );
    }

    @Test
    @DisplayName("Should shutdown cleanly with pending tasks")
    void shutdown_shouldCompleteGracefully() throws InterruptedException {
        CountDownLatch replicationStarted = new CountDownLatch(1);
        CountDownLatch shutdownLatch = new CountDownLatch(1);

        doAnswer(invocation -> {
            replicationStarted.countDown();
            shutdownLatch.await();
            return null;
        }).when(sender).replicate(any(), any());

        manager.onLocalSet("key", "value");
        replicationStarted.await();

        Thread shutdownThread = new Thread(() -> manager.shutdown());
        shutdownThread.start();

        Thread.sleep(50);
        shutdownLatch.countDown();

        shutdownThread.join(5000);
        assertFalse(shutdownThread.isAlive(), "Shutdown should complete");
        verify(sender).shutdown();
    }

    @Test
    @DisplayName("Should preserve task order for same key")
    void shouldPreserveOrderForSameKey() throws InterruptedException {
        String key = "ordered-key";

        CountDownLatch latch = new CountDownLatch(3);
        doAnswer(invocation -> {
            latch.countDown();
            return null;
        }).when(sender).replicate(any(), any());

        manager.onLocalSet(key, "value1");
        manager.onLocalSet(key, "value2");
        manager.onLocalDelete(key);

        assertTrue(latch.await(ASYNC_TIMEOUT_MS, TimeUnit.MILLISECONDS));

        ArgumentCaptor<ReplicationTask> taskCaptor = ArgumentCaptor.forClass(ReplicationTask.class);
        verify(sender, timeout(ASYNC_TIMEOUT_MS).times(3)).replicate(eq(currentNode), taskCaptor.capture());

        var tasks = taskCaptor.getAllValues();
        assertEquals(3, tasks.size());
        assertEquals("value1", tasks.get(0).value());
        assertEquals("value2", tasks.get(1).value());
        assertEquals(ReplicationTask.Operation.DELETE, tasks.get(2).operation());
    }
}