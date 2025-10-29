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
        receiver = new ReplicationReceiver("127.0.0.1", 0, store, 1024 * 1024, "node-A");
    }

    @Test
    @DisplayName("applyTask() should apply SET task to store")
    void shouldApplySetTask() {
        ReplicationTask task = ReplicationTask.ofSet("key-1", "value-1", "node-B");

        receiver.applyTask(task);

        verify(store, times(1)).put("key-1", "value-1", task.timestamp());
    }

    @Test
    @DisplayName("applyTask() should apply DELETE task to store")
    void shouldApplyDeleteTask() {
        ReplicationTask task = ReplicationTask.ofDelete("key-2", "node-B");

        receiver.applyTask(task);

        verify(store, times(1)).delete("key-2");
    }

    @Test
    @DisplayName("applyTask() should ignore self-origin tasks to prevent loops")
    void shouldIgnoreSelfOriginTasks() {
        ReplicationTask selfTask = ReplicationTask.ofSet("key-self", "value-self", "node-A");

        receiver.applyTask(selfTask);

        verify(store, never()).put(anyString(), anyString(), anyLong());
        verify(store, never()).delete(anyString());
    }

    @Test
    @DisplayName("applyTask() should apply tasks from different origins")
    void shouldApplyTasksFromDifferentOrigins() {
        ReplicationTask externalTask = ReplicationTask.ofSet("key-ext", "value-ext", "node-B");

        receiver.applyTask(externalTask);

        verify(store, times(1)).put("key-ext", "value-ext", externalTask.timestamp());
    }

    @Test
    @DisplayName("applyTask() should handle null task gracefully")
    void shouldHandleNullTask() {
        assertDoesNotThrow(() -> receiver.applyTask(null));

        verify(store, never()).put(anyString(), anyString(), anyLong());
        verify(store, never()).delete(anyString());
    }

    @Test
    @DisplayName("applyTask() should apply task when receiver has no nodeId")
    void shouldApplyTaskWhenNodeIdIsNull() {
        ReplicationReceiver receiverNoId = new ReplicationReceiver("127.0.0.1", 0, store);

        ReplicationTask task = ReplicationTask.ofSet("key", "value", "any-origin");
        receiverNoId.applyTask(task);

        verify(store, times(1)).put("key", "value", task.timestamp());
    }

    @Test
    @DisplayName("applyTask() should ignore task when origin exactly matches nodeId")
    void shouldIgnoreTaskWhenOriginMatchesNodeId() {
        String nodeId = "node-X";
        ReplicationReceiver receiverX = new ReplicationReceiver("127.0.0.1", 0, store, 1024, nodeId);

        ReplicationTask taskFromSelf = ReplicationTask.ofSet("k", "v", nodeId);
        receiverX.applyTask(taskFromSelf);

        verify(store, never()).put(anyString(), anyString(), anyLong());
    }

    @Test
    @DisplayName("applyTask() should handle multiple SET tasks sequentially")
    void shouldHandleMultipleSetTasks() {
        ReplicationTask task1 = ReplicationTask.ofSet("k1", "v1", "node-B");
        ReplicationTask task2 = ReplicationTask.ofSet("k2", "v2", "node-B");
        ReplicationTask task3 = ReplicationTask.ofSet("k3", "v3", "node-C");

        receiver.applyTask(task1);
        receiver.applyTask(task2);
        receiver.applyTask(task3);

        verify(store, times(1)).put("k1", "v1", task1.timestamp());
        verify(store, times(1)).put("k2", "v2", task2.timestamp());
        verify(store, times(1)).put("k3", "v3", task3.timestamp());
    }

    @Test
    @DisplayName("applyTask() should handle multiple DELETE tasks sequentially")
    void shouldHandleMultipleDeleteTasks() {
        ReplicationTask del1 = ReplicationTask.ofDelete("k1", "node-B");
        ReplicationTask del2 = ReplicationTask.ofDelete("k2", "node-C");

        receiver.applyTask(del1);
        receiver.applyTask(del2);

        verify(store, times(1)).delete("k1");
        verify(store, times(1)).delete("k2");
    }

    @Test
    @DisplayName("applyTask() should handle mixed SET and DELETE operations")
    void shouldHandleMixedOperations() {
        ReplicationTask set = ReplicationTask.ofSet("key", "value", "node-B");
        ReplicationTask delete = ReplicationTask.ofDelete("key", "node-B");

        receiver.applyTask(set);
        receiver.applyTask(delete);

        verify(store, times(1)).put("key", "value", set.timestamp());
        verify(store, times(1)).delete("key");
    }

    @Test
    @DisplayName("applyTask() should preserve timestamp from replication task")
    void shouldPreserveTimestamp() {
        long customTimestamp = 1234567890L;
        ReplicationTask task = new ReplicationTask("key", "value",
                ReplicationTask.Operation.SET, customTimestamp, "node-B");

        receiver.applyTask(task);

        verify(store, times(1)).put("key", "value", customTimestamp);
    }

    @Test
    @DisplayName("Constructor should throw NPE for null host")
    void shouldThrowOnNullHost() {
        assertThrows(NullPointerException.class,
                () -> new ReplicationReceiver(null, 9000, store));
    }

    @Test
    @DisplayName("Constructor should throw NPE for null store")
    void shouldThrowOnNullStore() {
        assertThrows(NullPointerException.class,
                () -> new ReplicationReceiver("127.0.0.1", 9000, null));
    }

    @Test
    @DisplayName("Constructor should accept all valid parameter combinations")
    void shouldAcceptValidConstructorParameters() {
        assertDoesNotThrow(() -> {
            new ReplicationReceiver("127.0.0.1", 9000, store);
            new ReplicationReceiver("127.0.0.1", 9000, store, 1024);
            new ReplicationReceiver("127.0.0.1", 9000, store, 1024, "node-id");
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
        doThrow(new RuntimeException("Store failure"))
                .when(store).put(anyString(), anyString(), anyLong());

        ReplicationTask task = ReplicationTask.ofSet("key", "value", "node-B");

        assertDoesNotThrow(() -> receiver.applyTask(task),
                "Should not propagate store exceptions");
    }
}