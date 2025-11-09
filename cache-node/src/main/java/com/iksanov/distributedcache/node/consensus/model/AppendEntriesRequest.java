package com.iksanov.distributedcache.node.consensus.model;

import java.util.List;
import java.util.Objects;

/**
 * AppendEntries RPC payload. Keep minimal and serializable.
 */
public record AppendEntriesRequest(long term, String leaderId, long prevLogIndex, long prevLogTerm,
                                   List<LogEntry> entries, long leaderCommit) {
    public AppendEntriesRequest(long term, String leaderId, long prevLogIndex, long prevLogTerm, List<LogEntry> entries, long leaderCommit) {
        this.term = term;
        this.leaderId = Objects.requireNonNull(leaderId);
        this.prevLogIndex = prevLogIndex;
        this.prevLogTerm = prevLogTerm;
        this.entries = entries;
        this.leaderCommit = leaderCommit;
    }

    @Override
    public String toString() {
        return "AppendEntriesRequest{term=" + term + ", leader=" + leaderId + ", prevIndex=" + prevLogIndex +
                ", entries=" + (entries == null ? 0 : entries.size()) + ", leaderCommit=" + leaderCommit + "}";
    }
}
