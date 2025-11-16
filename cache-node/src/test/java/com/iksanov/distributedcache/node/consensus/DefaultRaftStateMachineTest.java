package com.iksanov.distributedcache.node.consensus;

import com.iksanov.distributedcache.node.consensus.model.Command;
import com.iksanov.distributedcache.node.consensus.model.LogEntry;
import com.iksanov.distributedcache.node.consensus.raft.DefaultRaftStateMachine;
import com.iksanov.distributedcache.node.core.CacheStore;
import com.iksanov.distributedcache.node.metrics.RaftMetrics;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link DefaultRaftStateMachine}.
 * <p>
 * Covers:
 *  - SET/DELETE command application
 *  - GET operation from cache
 *  - Metrics tracking
 *  - Error handling
 */
@ExtendWith(MockitoExtension.class)
class DefaultRaftStateMachineTest {

    @Mock
    private CacheStore cacheStore;
    @Mock
    private RaftMetrics metrics;

    private DefaultRaftStateMachine stateMachine;

    @BeforeEach
    void setUp() {
        stateMachine = new DefaultRaftStateMachine(cacheStore, metrics);
    }

    @Test
    @DisplayName("Should apply SET command to cache store")
    void shouldApplySetCommand() {
        Command cmd = new Command(Command.Type.SET, "key1", "value1");
        LogEntry entry = new LogEntry(1, 1, cmd);

        stateMachine.apply(entry);

        verify(cacheStore, times(1)).put("key1", "value1");
        verify(metrics, times(1)).incrementCommandsApplied();
    }

    @Test
    @DisplayName("Should apply DELETE command to cache store")
    void shouldApplyDeleteCommand() {
        Command cmd = new Command(Command.Type.DELETE, "key2", null);
        LogEntry entry = new LogEntry(2, 1, cmd);

        stateMachine.apply(entry);

        verify(cacheStore, times(1)).delete("key2");
        verify(metrics, times(1)).incrementCommandsApplied();
    }

    @Test
    @DisplayName("Should handle multiple commands in sequence")
    void shouldHandleMultipleCommands() {
        LogEntry entry1 = new LogEntry(1, 1, new Command(Command.Type.SET, "a", "1"));
        LogEntry entry2 = new LogEntry(2, 1, new Command(Command.Type.SET, "b", "2"));
        LogEntry entry3 = new LogEntry(3, 1, new Command(Command.Type.DELETE, "a", null));

        stateMachine.apply(entry1);
        stateMachine.apply(entry2);
        stateMachine.apply(entry3);

        verify(cacheStore, times(1)).put("a", "1");
        verify(cacheStore, times(1)).put("b", "2");
        verify(cacheStore, times(1)).delete("a");
        verify(metrics, times(3)).incrementCommandsApplied();
    }

    @Test
    @DisplayName("Should delegate GET to cache store")
    void shouldDelegateGetToCacheStore() {
        when(cacheStore.get("key3")).thenReturn("value3");

        String result = stateMachine.get("key3");

        assertEquals("value3", result);
        verify(cacheStore, times(1)).get("key3");
    }

    @Test
    @DisplayName("Should return null when key not found")
    void shouldReturnNullWhenKeyNotFound() {
        when(cacheStore.get("missing")).thenReturn(null);

        String result = stateMachine.get("missing");

        assertNull(result);
        verify(cacheStore, times(1)).get("missing");
    }

    @Test
    @DisplayName("Should handle cache store exceptions during apply")
    void shouldHandleCacheStoreExceptionsDuringApply() {
        Command cmd = new Command(Command.Type.SET, "badkey", "badvalue");
        LogEntry entry = new LogEntry(1, 1, cmd);

        doThrow(new RuntimeException("Cache error")).when(cacheStore).put("badkey", "badvalue");

        // Should not throw - just log and continue
        assertDoesNotThrow(() -> stateMachine.apply(entry));

        verify(cacheStore, times(1)).put("badkey", "badvalue");
        // Metrics should still be incremented (or not, depending on implementation)
    }

    // Note: Cannot test null command because LogEntry constructor requires non-null command

    @Test
    @DisplayName("Should apply SET with empty value")
    void shouldApplySetWithEmptyValue() {
        Command cmd = new Command(Command.Type.SET, "emptyKey", "");
        LogEntry entry = new LogEntry(1, 1, cmd);

        stateMachine.apply(entry);

        verify(cacheStore, times(1)).put("emptyKey", "");
        verify(metrics, times(1)).incrementCommandsApplied();
    }

    @Test
    @DisplayName("Should handle GET with null key")
    void shouldHandleGetWithNullKey() {
        when(cacheStore.get(null)).thenReturn(null);

        String result = stateMachine.get(null);

        assertNull(result);
        verify(cacheStore, times(1)).get(null);
    }

    @Test
    @DisplayName("Should verify metrics are called correctly")
    void shouldVerifyMetricsCalledCorrectly() {
        LogEntry entry1 = new LogEntry(1, 1, new Command(Command.Type.SET, "k1", "v1"));
        LogEntry entry2 = new LogEntry(2, 1, new Command(Command.Type.DELETE, "k2", null));

        stateMachine.apply(entry1);
        stateMachine.apply(entry2);

        verify(metrics, times(2)).incrementCommandsApplied();
        verify(metrics, times(2)).setLastApplied(anyLong());
        verifyNoMoreInteractions(metrics);
    }

    @Test
    @DisplayName("Should apply commands with different terms")
    void shouldApplyCommandsWithDifferentTerms() {
        LogEntry entry1 = new LogEntry(1, 1, new Command(Command.Type.SET, "k1", "v1"));
        LogEntry entry2 = new LogEntry(2, 2, new Command(Command.Type.SET, "k2", "v2"));
        LogEntry entry3 = new LogEntry(3, 3, new Command(Command.Type.DELETE, "k1", null));

        stateMachine.apply(entry1);
        stateMachine.apply(entry2);
        stateMachine.apply(entry3);

        verify(cacheStore, times(1)).put("k1", "v1");
        verify(cacheStore, times(1)).put("k2", "v2");
        verify(cacheStore, times(1)).delete("k1");
        verify(metrics, times(3)).incrementCommandsApplied();
    }

    // ========== SNAPSHOT TESTS ==========

    @Test
    @DisplayName("Should create snapshot from cache store")
    void shouldCreateSnapshotFromCacheStore() {
        java.util.Map<String, String> expectedData = new java.util.HashMap<>();
        expectedData.put("snap1", "value1");
        expectedData.put("snap2", "value2");

        when(cacheStore.exportData()).thenReturn(expectedData);

        java.util.Map<String, String> snapshot = stateMachine.createSnapshot();

        assertNotNull(snapshot);
        assertEquals(2, snapshot.size());
        assertEquals("value1", snapshot.get("snap1"));
        assertEquals("value2", snapshot.get("snap2"));
        verify(cacheStore, times(1)).exportData();
    }

    @Test
    @DisplayName("Should create empty snapshot when cache is empty")
    void shouldCreateEmptySnapshotWhenCacheIsEmpty() {
        when(cacheStore.exportData()).thenReturn(new java.util.HashMap<>());

        java.util.Map<String, String> snapshot = stateMachine.createSnapshot();

        assertNotNull(snapshot);
        assertTrue(snapshot.isEmpty());
        verify(cacheStore, times(1)).exportData();
    }

    @Test
    @DisplayName("Should restore snapshot to cache store")
    void shouldRestoreSnapshotToCacheStore() {
        java.util.Map<String, String> snapshotData = new java.util.HashMap<>();
        snapshotData.put("restored1", "value1");
        snapshotData.put("restored2", "value2");

        stateMachine.restoreSnapshot(snapshotData);

        verify(cacheStore, times(1)).importData(snapshotData);
    }

    @Test
    @DisplayName("Should restore empty snapshot")
    void shouldRestoreEmptySnapshot() {
        java.util.Map<String, String> emptySnapshot = new java.util.HashMap<>();

        stateMachine.restoreSnapshot(emptySnapshot);

        verify(cacheStore, times(1)).importData(emptySnapshot);
    }

    @Test
    @DisplayName("Should throw exception when restoring null snapshot")
    void shouldThrowExceptionWhenRestoringNullSnapshot() {
        assertThrows(NullPointerException.class, () -> stateMachine.restoreSnapshot(null));
    }

    @Test
    @DisplayName("Should create snapshot with large dataset")
    void shouldCreateSnapshotWithLargeDataset() {
        java.util.Map<String, String> largeData = new java.util.HashMap<>();
        for (int i = 0; i < 10000; i++) {
            largeData.put("key" + i, "value" + i);
        }

        when(cacheStore.exportData()).thenReturn(largeData);

        java.util.Map<String, String> snapshot = stateMachine.createSnapshot();

        assertEquals(10000, snapshot.size());
        verify(cacheStore, times(1)).exportData();
    }

    @Test
    @DisplayName("Should handle snapshot creation and restoration cycle")
    void shouldHandleSnapshotCreationAndRestorationCycle() {
        // Setup: Apply some commands
        LogEntry entry1 = new LogEntry(1, 1, new Command(Command.Type.SET, "a", "1"));
        LogEntry entry2 = new LogEntry(2, 1, new Command(Command.Type.SET, "b", "2"));
        stateMachine.apply(entry1);
        stateMachine.apply(entry2);

        // Create snapshot
        java.util.Map<String, String> stateData = new java.util.HashMap<>();
        stateData.put("a", "1");
        stateData.put("b", "2");
        when(cacheStore.exportData()).thenReturn(stateData);

        java.util.Map<String, String> snapshot = stateMachine.createSnapshot();

        // Restore snapshot (simulate recovery)
        stateMachine.restoreSnapshot(snapshot);

        verify(cacheStore, times(1)).exportData();
        verify(cacheStore, times(1)).importData(snapshot);
    }

    @Test
    @DisplayName("Should preserve special characters in snapshot")
    void shouldPreserveSpecialCharactersInSnapshot() {
        java.util.Map<String, String> dataWithSpecialChars = new java.util.HashMap<>();
        dataWithSpecialChars.put("key\nwith\nnewlines", "value\twith\ttabs");
        dataWithSpecialChars.put("special!@#$", "chars%^&*()");
        dataWithSpecialChars.put("normal", "");

        when(cacheStore.exportData()).thenReturn(dataWithSpecialChars);
        java.util.Map<String, String> snapshot = stateMachine.createSnapshot();

        stateMachine.restoreSnapshot(snapshot);

        verify(cacheStore).importData(argThat(map ->
            map.size() == 3 &&
            "value\twith\ttabs".equals(map.get("key\nwith\nnewlines")) &&
            "chars%^&*()".equals(map.get("special!@#$")) &&
            "".equals(map.get("normal"))
        ));
    }
}