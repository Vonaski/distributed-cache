package com.iksanov.distributedcache.node.consensus;

import com.iksanov.distributedcache.node.consensus.persistence.FileBasedRaftPersistence;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import java.io.IOException;
import java.nio.file.Path;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for FileBasedRaftPersistence.
 * Critical for ensuring Raft safety guarantees.
 */
@DisplayName("FileBasedRaftPersistence Tests")
class FileBasedRaftPersistenceTest {

    @TempDir
    Path tempDir;

    @Test
    @DisplayName("Should initialize with default values")
    void shouldInitializeWithDefaults() throws IOException {
        FileBasedRaftPersistence persistence = new FileBasedRaftPersistence(tempDir, "node-1");
        assertEquals(0L, persistence.loadTerm());
        assertNull(persistence.loadVotedFor());
    }

    @Test
    @DisplayName("Should save and load term")
    void shouldSaveAndLoadTerm() throws IOException {
        FileBasedRaftPersistence persistence = new FileBasedRaftPersistence(tempDir, "node-1");
        persistence.saveTerm(42L);
        assertEquals(42L, persistence.loadTerm());
        persistence.saveTerm(Long.MAX_VALUE);
        assertEquals(Long.MAX_VALUE, persistence.loadTerm());
    }

    @Test
    @DisplayName("Should save and load votedFor")
    void shouldSaveAndLoadVotedFor() throws IOException {
        FileBasedRaftPersistence persistence = new FileBasedRaftPersistence(tempDir, "node-1");
        persistence.saveVotedFor("node-leader");
        assertEquals("node-leader", persistence.loadVotedFor());
        persistence.saveVotedFor(null);
        assertNull(persistence.loadVotedFor());
    }

    @Test
    @DisplayName("Should persist data across recreations")
    void shouldPersistAcrossRecreations() throws IOException {
        FileBasedRaftPersistence persistence1 = new FileBasedRaftPersistence(tempDir, "node-1");
        persistence1.saveTerm(999L);
        persistence1.saveVotedFor("node-X");
        FileBasedRaftPersistence persistence2 = new FileBasedRaftPersistence(tempDir, "node-1");
        assertEquals(999L, persistence2.loadTerm());
        assertEquals("node-X", persistence2.loadVotedFor());
    }

    @Test
    @DisplayName("Should handle saveBoth atomically")
    void shouldHandleSaveBoth() throws IOException {
        FileBasedRaftPersistence persistence = new FileBasedRaftPersistence(tempDir, "node-1");
        persistence.saveBoth(777L, "node-batch");
        assertEquals(777L, persistence.loadTerm());
        assertEquals("node-batch", persistence.loadVotedFor());
        persistence.saveBoth(888L, null);
        assertEquals(888L, persistence.loadTerm());
        assertNull(persistence.loadVotedFor());
    }

    @Test
    @DisplayName("Should isolate different node IDs")
    void shouldIsolateDifferentNodes() throws IOException {
        FileBasedRaftPersistence persistence1 = new FileBasedRaftPersistence(tempDir, "node-1");
        FileBasedRaftPersistence persistence2 = new FileBasedRaftPersistence(tempDir, "node-2");
        persistence1.saveTerm(10L);
        persistence1.saveVotedFor("leader-1");
        persistence2.saveTerm(20L);
        persistence2.saveVotedFor("leader-2");
        assertEquals(10L, persistence1.loadTerm());
        assertEquals("leader-1", persistence1.loadVotedFor());
        assertEquals(20L, persistence2.loadTerm());
        assertEquals("leader-2", persistence2.loadVotedFor());
    }

    @Test
    @DisplayName("Should reject excessively long votedFor")
    void shouldRejectTooLongVotedFor() throws IOException {
        FileBasedRaftPersistence persistence = new FileBasedRaftPersistence(tempDir, "node-1");
        String tooLong = "x".repeat(300); // Max is 255
        assertThrows(IOException.class, () -> persistence.saveVotedFor(tooLong));
    }

    @Test
    @DisplayName("Should handle rapid updates correctly")
    void shouldHandleRapidUpdates() throws IOException {
        FileBasedRaftPersistence persistence = new FileBasedRaftPersistence(tempDir, "node-1");
        for (long term = 1; term <= 100; term++) {
            persistence.saveTerm(term);
            assertEquals(term, persistence.loadTerm());
        }
    }
}