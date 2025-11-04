package com.iksanov.distributedcache.node.consensus;

import java.io.IOException;

public interface RaftPersistence {
    void saveTerm(long term) throws IOException;
    long loadTerm() throws IOException;
    void saveVotedFor(String nodeId) throws IOException;
    String loadVotedFor() throws IOException;

    static RaftPersistence noop() {
        return new RaftPersistence() {
            public void saveTerm(long term) {}
            public long loadTerm() { return 0L; }
            public void saveVotedFor(String nodeId) {}
            public String loadVotedFor() { return null; }
        };
    }
}
