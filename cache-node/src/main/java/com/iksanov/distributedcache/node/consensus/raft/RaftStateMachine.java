package com.iksanov.distributedcache.node.consensus.raft;

import com.iksanov.distributedcache.node.consensus.model.LogEntry;

/**
 * RaftStateMachine defines how committed log entries are applied to actual storage.
 */
public interface RaftStateMachine {

    void apply(LogEntry entry);
}
