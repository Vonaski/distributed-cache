package com.iksanov.distributedcache.node.consensus.raft;

import com.iksanov.distributedcache.node.consensus.model.LogEntry;
import java.util.Map;

/**
 * Called by RaftNode from a single-threaded executor in commit order.
 * Implementations must be idempotent and thread-safe for concurrent reads.
 */
public interface RaftStateMachine {

    void apply(LogEntry entry);
    long getLastAppliedIndex();
    String get(String key);
    int size();

    /**
     * Creates a snapshot of the current state machine state.
     * @return a map containing all key-value pairs in the state machine
     */
    Map<String, String> createSnapshot();

    /**
     * Restores the state machine from a snapshot.
     * @param snapshotData the snapshot data to restore from
     */
    void restoreSnapshot(Map<String, String> snapshotData);
}
