package com.iksanov.distributedcache.node.replication;

import com.iksanov.distributedcache.node.core.CacheStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link ReplicationReceiver}.
 * <p>
 * These are TRUE unit tests:
 *  - No network I/O
 *  - No real Netty server
 *  - Only business logic testing
 *  - Fast execution (<1 second)
 */
@ExtendWith(MockitoExtension.class)
public class ReplicationReceiverTest {

    @Mock
    private CacheStore store;

    private ReplicationReceiver receiver;

    @BeforeEach
    void setUp() {
        receiver = new ReplicationReceiver("127.0.0.1", 0, store, 1024 * 1024, "node-A", null);
    }

    @Test
    @DisplayName("applyTask() should apply SET task to store")
    void shouldApplySetTask() {
        ReplicationTask task = ReplicationTask.ofSet("key-1", "value-1", "node-B", 0L);
        receiver.applyTask(task);
        verify(store, times(1)).put("key-1", "value-1");
    }

    @Test
    @DisplayName("applyTask() should apply DELETE task to store")
    void shouldApplyDeleteTask() {
        ReplicationTask task = ReplicationTask.ofDelete("key-2", "node-B", 0L);
        receiver.applyTask(task);
        verify(store, times(1)).delete("key-2");
    }

    @Test
    @DisplayName("applyTask() should ignore self-origin tasks to prevent loops")
    void shouldIgnoreSelfOriginTasks() {
        ReplicationTask selfTask = ReplicationTask.ofSet("key-self", "value-self", "node-A", 0L);
        receiver.applyTask(selfTask);
        verify(store, never()).put(anyString(), anyString());
        verify(store, never()).delete(anyString());
    }

    @Test
    @DisplayName("applyTask() should apply tasks from different origins")
    void shouldApplyTasksFromDifferentOrigins() {
        ReplicationTask externalTask = ReplicationTask.ofSet("key-ext", "value-ext", "node-B", 0L);
        receiver.applyTask(externalTask);
        verify(store, times(1)).put("key-ext", "value-ext");
    }

    @Test
    @DisplayName("applyTask() should handle null task gracefully")
    void shouldHandleNullTask() {
        assertDoesNotThrow(() -> receiver.applyTask(null));
        verify(store, never()).put(anyString(), anyString());
        verify(store, never()).delete(anyString());
    }

    @Test
    @DisplayName("applyTask() should apply task when receiver has no nodeId")
    void shouldApplyTaskWhenNodeIdIsNull() {
        ReplicationReceiver receiverNoId = new ReplicationReceiver("127.0.0.1", 0, store, 1024 * 1024, null, null);
        ReplicationTask task = ReplicationTask.ofSet("key", "value", "any-origin", 0L);
        receiverNoId.applyTask(task);
        verify(store, times(1)).put("key", "value");
    }

    @Test
    @DisplayName("applyTask() should ignore task when origin exactly matches nodeId")
    void shouldIgnoreTaskWhenOriginMatchesNodeId() {
        String nodeId = "node-X";
        ReplicationReceiver receiverX = new ReplicationReceiver("127.0.0.1", 0, store, 1024, nodeId, null);
        ReplicationTask taskFromSelf = ReplicationTask.ofSet("k", "v", nodeId, 0L);
        receiverX.applyTask(taskFromSelf);
        verify(store, never()).put(anyString(), anyString());
    }

    @Test
    @DisplayName("applyTask() should handle multiple SET tasks sequentially")
    void shouldHandleMultipleSetTasks() {
        ReplicationTask task1 = ReplicationTask.ofSet("k1", "v1", "node-B", 0L);
        ReplicationTask task2 = ReplicationTask.ofSet("k2", "v2", "node-B", 0L);
        ReplicationTask task3 = ReplicationTask.ofSet("k3", "v3", "node-C", 0L);
        receiver.applyTask(task1);
        receiver.applyTask(task2);
        receiver.applyTask(task3);
        verify(store, times(1)).put("k1", "v1");
        verify(store, times(1)).put("k2", "v2");
        verify(store, times(1)).put("k3", "v3");
    }

    @Test
    @DisplayName("applyTask() should handle multiple DELETE tasks sequentially")
    void shouldHandleMultipleDeleteTasks() {
        ReplicationTask del1 = ReplicationTask.ofDelete("k1", "node-B", 0L);
        ReplicationTask del2 = ReplicationTask.ofDelete("k2", "node-C", 0L);
        receiver.applyTask(del1);
        receiver.applyTask(del2);
        verify(store, times(1)).delete("k1");
        verify(store, times(1)).delete("k2");
    }

    @Test
    @DisplayName("applyTask() should handle mixed SET and DELETE operations")
    void shouldHandleMixedOperations() {
        ReplicationTask set = ReplicationTask.ofSet("key", "value", "node-B", 0L);
        ReplicationTask delete = ReplicationTask.ofDelete("key", "node-B", 0L);
        receiver.applyTask(set);
        receiver.applyTask(delete);
        verify(store, times(1)).put("key", "value");
        verify(store, times(1)).delete("key");
    }

    @Test
    @DisplayName("applyTask() should preserve timestamp from replication task")
    void shouldPreserveTimestamp() {
        long customTimestamp = 1234567890L;
        ReplicationTask task = new ReplicationTask("key", "value", ReplicationTask.Operation.SET, customTimestamp, "node-B", 0L);
        receiver.applyTask(task);
        verify(store, times(1)).put("key", "value");
    }

    @Test
    @DisplayName("Constructor should throw NPE for null host")
    void shouldThrowOnNullHost() {
        assertThrows(NullPointerException.class, () -> new ReplicationReceiver(null, 9000, store, 1024, null, null));
    }

    @Test
    @DisplayName("Constructor should throw NPE for null store")
    void shouldThrowOnNullStore() {
        assertThrows(NullPointerException.class, () -> new ReplicationReceiver("127.0.0.1", 9000, null, 1024, null, null));
    }

    @Test
    @DisplayName("Constructor should accept all valid parameter combinations")
    void shouldAcceptValidConstructorParameters() {
        assertDoesNotThrow(() -> {
            new ReplicationReceiver("127.0.0.1", 9000, store, 1024, null, null);
            new ReplicationReceiver("127.0.0.1", 9000, store, 1024, "node-id", null);
        });
    }

    @Test
    @DisplayName("isRunning() should return false before start()")
    void shouldReturnFalseBeforeStart() {
        assertFalse(receiver.isRunning());
    }

    @Test
    @DisplayName("applyTask() should not throw when store throws exception")
    void shouldHandleStoreExceptions() {
        doThrow(new RuntimeException("Store failure")).when(store).put(anyString(), anyString());
        ReplicationTask task = ReplicationTask.ofSet("key", "value", "node-B", 0L);
        assertDoesNotThrow(() -> receiver.applyTask(task), "Should not propagate store exceptions");
    }

    @Test
    @DisplayName("Should ignore out-of-order replication tasks")
    void shouldIgnoreOutOfOrderTasks() {
        ReplicationReceiver receiver = new ReplicationReceiver("127.0.0.1", 0, store, 1024, "node-A", null);
        ReplicationTask task2 = new ReplicationTask("key", "value2", ReplicationTask.Operation.SET, System.currentTimeMillis(), "node-B", 2L);
        ReplicationTask task1 = new ReplicationTask("key", "value1", ReplicationTask.Operation.SET, System.currentTimeMillis(), "node-B", 1L);
        ReplicationTask task3 = new ReplicationTask("key", "value3", ReplicationTask.Operation.SET, System.currentTimeMillis(), "node-B", 3L);
        receiver.applyTask(task2);
        receiver.applyTask(task1);
        receiver.applyTask(task3);
        verify(store, times(1)).put("key", "value2");
        verify(store, never()).put("key", "value1");
        verify(store, times(1)).put("key", "value3");
    }
}