package com.iksanov.distributedcache.node.consensus.persistence;

import java.io.IOException;

public interface RaftPersistence {

    void saveTerm(long term) throws IOException;
    long loadTerm() throws IOException;
    void saveVotedFor(String nodeId) throws IOException;
    String loadVotedFor() throws IOException;
    void saveBoth(long term, String votedFor) throws IOException;
}
