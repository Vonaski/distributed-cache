package com.iksanov.distributedcache.node.consensus.persistence;

import com.iksanov.distributedcache.node.consensus.model.LogEntry;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public interface RaftPersistence {

    void saveTerm(long term) throws IOException;
    long loadTerm() throws IOException;
    void saveVotedFor(String nodeId) throws IOException;
    String loadVotedFor() throws IOException;
    void saveBoth(long term, String votedFor) throws IOException;
    void appendLogEntry(LogEntry entry) throws IOException;
    void appendLogEntries(List<LogEntry> entries) throws IOException;
    List<LogEntry> loadLog() throws IOException;
    void truncateLog(long fromIndex) throws IOException;
    long getLogSize() throws IOException;
    void saveSnapshot(long lastIncludedIndex, long lastIncludedTerm, Map<String, String> data) throws IOException;
    Map<String, String> loadSnapshotData() throws IOException;
    long getSnapshotLastIncludedIndex() throws IOException;
    long getSnapshotLastIncludedTerm() throws IOException;
    boolean hasSnapshot() throws IOException;
    void deleteOldSnapshots() throws IOException;
}
