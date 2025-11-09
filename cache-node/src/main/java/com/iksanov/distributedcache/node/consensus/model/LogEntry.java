package com.iksanov.distributedcache.node.consensus.model;

import java.util.Objects;

/**
 * One Raft log entry. Keep it serializable-friendly.
 */
public record LogEntry(long index, long term, Command command) {
    public LogEntry(long index, long term, Command command) {
        this.index = index;
        this.term = term;
        this.command = Objects.requireNonNull(command);
    }

    @Override
    public String toString() {
        return "RaftLogEntry{" + "index=" + index + ", term=" + term + ", cmd=" + command + '}';
    }
}
