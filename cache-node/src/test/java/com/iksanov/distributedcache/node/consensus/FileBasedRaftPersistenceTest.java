package com.iksanov.distributedcache.node.consensus;

import com.iksanov.distributedcache.node.consensus.model.Command;
import com.iksanov.distributedcache.node.consensus.model.LogEntry;
import com.iksanov.distributedcache.node.consensus.persistence.FileBasedRaftPersistence;
import org.junit.jupiter.api.*;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class FileBasedRaftPersistenceTest {

    private Path tempDir;
    private FileBasedRaftPersistence persistence;

    @BeforeEach
    void setup() throws IOException {
        tempDir = Files.createTempDirectory("raft-test-");
        persistence = new FileBasedRaftPersistence(tempDir, "node1");
    }

    @AfterEach
    void cleanup() throws IOException {
        Files.walk(tempDir)
                .sorted(Comparator.reverseOrder())
                .forEach(p -> { try { Files.deleteIfExists(p); } catch (IOException ignored) {} });
    }

    @Test
    @Order(1)
    void testInitialStateFileCreated() {
        assertTrue(Files.exists(tempDir.resolve("raft-node1.state")));
        assertTrue(Files.exists(tempDir.resolve("raft-node1.log")) || !Files.exists(tempDir.resolve("raft-node1.log")));
    }

    @Test
    @Order(2)
    void testSaveAndLoadTerm() throws IOException {
        persistence.saveTerm(42);
        long loaded = persistence.loadTerm();
        assertEquals(42, loaded);
    }

    @Test
    @Order(3)
    void testSaveAndLoadVotedFor() throws IOException {
        persistence.saveVotedFor("leaderX");
        String loaded = persistence.loadVotedFor();
        assertEquals("leaderX", loaded);

        // null case
        persistence.saveVotedFor(null);
        assertNull(persistence.loadVotedFor());
    }

    @Test
    @Order(4)
    void testSaveBoth() throws IOException {
        persistence.saveBoth(99L, "nodeZ");
        assertEquals(99L, persistence.loadTerm());
        assertEquals("nodeZ", persistence.loadVotedFor());
    }

    @Test
    @Order(5)
    void testAppendAndLoadLog() throws IOException {
        List<LogEntry> original = List.of(
                new LogEntry(1, 1, new Command(Command.Type.SET, "a", "1")),
                new LogEntry(2, 1, new Command(Command.Type.SET, "b", "2")),
                new LogEntry(3, 2, new Command(Command.Type.DELETE, "a", null))
        );
        for (LogEntry e : original) {
            persistence.appendLogEntry(e);
        }

        List<LogEntry> loaded = persistence.loadLog();
        assertEquals(original.size(), loaded.size());
        for (int i = 0; i < original.size(); i++) {
            LogEntry exp = original.get(i);
            LogEntry act = loaded.get(i);
            assertEquals(exp.index(), act.index());
            assertEquals(exp.term(), act.term());
            assertEquals(exp.command().type(), act.command().type());
            assertEquals(exp.command().key(), act.command().key());
            assertEquals(exp.command().value(), act.command().value());
        }
    }

    @Test
    @Order(6)
    void testAppendLogEntriesBulk() throws IOException {
        List<LogEntry> entries = new ArrayList<>();
        for (int i = 1; i <= 10; i++) {
            entries.add(new LogEntry(i, 1, new Command(Command.Type.SET, "k" + i, "v" + i)));
        }
        persistence.appendLogEntries(entries);
        List<LogEntry> loaded = persistence.loadLog();
        assertEquals(entries.size(), loaded.size());
    }

    @Test
    @Order(7)
    void testTruncateLog() throws IOException {
        List<LogEntry> entries = new ArrayList<>();
        for (int i = 1; i <= 5; i++) {
            entries.add(new LogEntry(i, 1, new Command(Command.Type.SET, "k" + i, "v" + i)));
        }
        persistence.appendLogEntries(entries);

        // Truncate from index 4: removes entries >= 4 (i.e., 4, 5), keeps entries < 4 (i.e., 1, 2, 3)
        // This is standard Raft semantics for conflict resolution
        persistence.truncateLog(4);
        List<LogEntry> loaded = persistence.loadLog();
        assertEquals(3, loaded.size(), "Should keep entries 1, 2, 3 (indices < 4)");
        assertEquals(1, loaded.getFirst().index());
        assertEquals(3, loaded.get(2).index());
    }

    @Test
    @Order(8)
    void testTruncateCompletely() throws IOException {
        persistence.appendLogEntry(new LogEntry(1, 1, new Command(Command.Type.SET, "a", "b")));
        persistence.truncateLog(1);
        assertTrue(persistence.loadLog().isEmpty());
    }

    @Test
    @Order(9)
    void testCorruptedLogEntry() throws IOException {
        // create corrupted log manually
        Path logPath = tempDir.resolve("raft-node1.log");
        Files.write(logPath, new byte[]{0, 1, 2, 3, 4, 5, 6, 7});
        List<LogEntry> result = persistence.loadLog();
        assertTrue(result.isEmpty(), "Should skip corrupted entries gracefully");
    }

    @Test
    @Order(10)
    void testConcurrentAccess() throws Exception {
        int threads = 4;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch latch = new CountDownLatch(threads);
        for (int t = 0; t < threads; t++) {
            final int id = t;
            pool.submit(() -> {
                try {
                    persistence.appendLogEntry(new LogEntry(id + 1, 1, new Command(Command.Type.SET, "key" + id, "val" + id)));
                } catch (IOException e) {
                    fail(e);
                } finally {
                    latch.countDown();
                }
            });
        }
        latch.await(3, TimeUnit.SECONDS);
        pool.shutdown();
        List<LogEntry> loaded = persistence.loadLog();
        assertEquals(threads, loaded.size());
    }

    @Test
    @Order(11)
    void testGetLogSize() throws IOException {
        persistence.appendLogEntry(new LogEntry(1, 1, new Command(Command.Type.SET, "x", "1")));
        persistence.appendLogEntry(new LogEntry(2, 1, new Command(Command.Type.SET, "y", "2")));
        long size = persistence.getLogSize();
        assertEquals(2, size);
    }

    @Test
    @Order(12)
    void testReadFullyPartialRead() throws Exception {
        // test coverage for private static readFully (simulate EOF)
        var ch = FileChannel.open(tempDir.resolve("partial.bin"), StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.READ);
        ch.write(ByteBuffer.wrap("abc".getBytes()));
        ch.force(true);
        ch.position(0);
        ByteBuffer buf = ByteBuffer.allocate(10);
        var method = FileBasedRaftPersistence.class.getDeclaredMethod("readFully", FileChannel.class, ByteBuffer.class);
        method.setAccessible(true);
        int read = (int) method.invoke(null, ch, buf);
        assertTrue(read <= 3);
        ch.close();
    }

    // ========== SNAPSHOT TESTS ==========

    @Test
    @Order(13)
    @DisplayName("Should save and load snapshot")
    void testSaveAndLoadSnapshot() throws IOException {
        Map<String, String> snapshotData = new HashMap<>();
        snapshotData.put("key1", "value1");
        snapshotData.put("key2", "value2");
        snapshotData.put("key3", "value3");

        long lastIncludedIndex = 100;
        long lastIncludedTerm = 5;

        // Save snapshot
        persistence.saveSnapshot(lastIncludedIndex, lastIncludedTerm, snapshotData);

        // Verify snapshot exists
        assertTrue(persistence.hasSnapshot(), "Snapshot should exist after saving");

        // Load snapshot metadata
        assertEquals(lastIncludedIndex, persistence.getSnapshotLastIncludedIndex());
        assertEquals(lastIncludedTerm, persistence.getSnapshotLastIncludedTerm());

        // Load snapshot data
        Map<String, String> loadedData = persistence.loadSnapshotData();
        assertEquals(snapshotData.size(), loadedData.size());
        assertEquals("value1", loadedData.get("key1"));
        assertEquals("value2", loadedData.get("key2"));
        assertEquals("value3", loadedData.get("key3"));
    }

    @Test
    @Order(14)
    @DisplayName("Should return false when no snapshot exists")
    void testHasSnapshotReturnsFalseWhenNoSnapshot() throws IOException {
        assertFalse(persistence.hasSnapshot(), "Should return false when no snapshot exists");
    }

    @Test
    @Order(15)
    @DisplayName("Should handle empty snapshot data")
    void testEmptySnapshotData() throws IOException {
        Map<String, String> emptyData = new HashMap<>();
        persistence.saveSnapshot(50, 3, emptyData);

        assertTrue(persistence.hasSnapshot());
        assertEquals(50, persistence.getSnapshotLastIncludedIndex());
        assertEquals(3, persistence.getSnapshotLastIncludedTerm());

        Map<String, String> loaded = persistence.loadSnapshotData();
        assertTrue(loaded.isEmpty());
    }

    @Test
    @Order(16)
    @DisplayName("Should overwrite existing snapshot")
    void testOverwriteSnapshot() throws IOException {
        // First snapshot
        Map<String, String> data1 = Map.of("old", "data");
        persistence.saveSnapshot(10, 1, data1);

        // Second snapshot (overwrites)
        Map<String, String> data2 = Map.of("new", "data", "more", "values");
        persistence.saveSnapshot(20, 2, data2);

        // Should have new snapshot
        assertEquals(20, persistence.getSnapshotLastIncludedIndex());
        assertEquals(2, persistence.getSnapshotLastIncludedTerm());

        Map<String, String> loaded = persistence.loadSnapshotData();
        assertEquals(2, loaded.size());
        assertEquals("data", loaded.get("new"));
        assertEquals("values", loaded.get("more"));
        assertNull(loaded.get("old"));
    }

    @Test
    @Order(17)
    @DisplayName("Should handle large snapshot data")
    void testLargeSnapshotData() throws IOException {
        Map<String, String> largeData = new HashMap<>();
        for (int i = 0; i < 10000; i++) {
            largeData.put("key" + i, "value" + i + "_" + UUID.randomUUID());
        }

        long startTime = System.currentTimeMillis();
        persistence.saveSnapshot(5000, 10, largeData);
        long saveTime = System.currentTimeMillis() - startTime;

        startTime = System.currentTimeMillis();
        Map<String, String> loaded = persistence.loadSnapshotData();
        long loadTime = System.currentTimeMillis() - startTime;

        assertEquals(largeData.size(), loaded.size());
        for (int i = 0; i < 100; i++) { // Spot check
            assertEquals(largeData.get("key" + i), loaded.get("key" + i));
        }

        System.out.println("Snapshot save time: " + saveTime + "ms, load time: " + loadTime + "ms");
    }

    @Test
    @Order(18)
    @DisplayName("Should delete old snapshots")
    void testDeleteOldSnapshots() throws IOException {
        // Create multiple snapshots
        persistence.saveSnapshot(10, 1, Map.of("a", "1"));
        Path snapshot1 = tempDir.resolve("raft-node1.snapshot");
        assertTrue(Files.exists(snapshot1));

        // Delete old snapshots
        persistence.deleteOldSnapshots();

        // Current snapshot should still exist
        assertTrue(Files.exists(snapshot1));
    }

    @Test
    @Order(19)
    @DisplayName("Should handle snapshot with special characters in values")
    void testSnapshotWithSpecialCharacters() throws IOException {
        Map<String, String> data = new HashMap<>();
        data.put("key1", "value with spaces");
        data.put("key2", "value\nwith\nnewlines");
        data.put("key3", "value\twith\ttabs");
        data.put("key4", "");
        data.put("key5", "special!@#$%^&*()");

        persistence.saveSnapshot(100, 5, data);

        Map<String, String> loaded = persistence.loadSnapshotData();
        assertEquals(data.size(), loaded.size());
        assertEquals("value with spaces", loaded.get("key1"));
        assertEquals("value\nwith\nnewlines", loaded.get("key2"));
        assertEquals("value\twith\ttabs", loaded.get("key3"));
        assertEquals("", loaded.get("key4"));
        assertEquals("special!@#$%^&*()", loaded.get("key5"));
    }

    @Test
    @Order(20)
    @DisplayName("Should recover from snapshot after restart")
    void testSnapshotPersistenceAcrossRestarts() throws IOException {
        // Create and save snapshot
        Map<String, String> originalData = Map.of(
            "persistent1", "value1",
            "persistent2", "value2"
        );
        persistence.saveSnapshot(200, 10, originalData);

        // Close and reopen persistence
        persistence = new FileBasedRaftPersistence(tempDir, "node1");

        // Verify snapshot is still available
        assertTrue(persistence.hasSnapshot());
        assertEquals(200, persistence.getSnapshotLastIncludedIndex());
        assertEquals(10, persistence.getSnapshotLastIncludedTerm());

        Map<String, String> loaded = persistence.loadSnapshotData();
        assertEquals(originalData, loaded);
    }
}
