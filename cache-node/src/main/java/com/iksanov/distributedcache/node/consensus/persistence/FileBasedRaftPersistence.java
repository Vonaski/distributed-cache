package com.iksanov.distributedcache.node.consensus.persistence;

import com.iksanov.distributedcache.node.consensus.model.Command;
import com.iksanov.distributedcache.node.consensus.model.LogEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * File-based persistence for Raft state (metadata + log).
 * Thread-safe, crash-resistant implementation using fsync.
 * <p>
 * State file format (fixed 512 bytes):
 * - Bytes 0-7: currentTerm (long, big-endian)
 * - Bytes 8-263: votedFor (UTF-8 string, max 255 chars)
 * - Bytes 264-511: reserved for future use
 * <p>
 * Log file format (append-only):
 * Each entry: [8 bytes index][8 bytes term][1 byte cmd type][4 bytes key_len][key][4 bytes value_len][value]
 */
public class FileBasedRaftPersistence implements RaftPersistence {

    private static final Logger log = LoggerFactory.getLogger(FileBasedRaftPersistence.class);
    private static final int FILE_SIZE = 512;
    private static final int TERM_OFFSET = 0;
    private static final int VOTED_FOR_OFFSET = 8;
    private static final int VOTED_FOR_MAX_LENGTH = 255;
    private static final int MAX_KEY_LENGTH = 1024;
    private static final int MAX_VALUE_LENGTH = 64 * 1024;
    private final Path stateFile;
    private final Path logFile;
    private final Path snapshotFile;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    public FileBasedRaftPersistence(Path dataDirectory, String nodeId) throws IOException {
        Files.createDirectories(dataDirectory);
        this.stateFile = dataDirectory.resolve("raft-" + nodeId + ".state");
        this.logFile = dataDirectory.resolve("raft-" + nodeId + ".log");
        this.snapshotFile = dataDirectory.resolve("raft-" + nodeId + ".snapshot");
        initializeStateFile();
        log.info("[Persistence] Initialized file-based persistence at {}", dataDirectory);
    }

    private void initializeStateFile() throws IOException {
        if (!Files.exists(stateFile)) {
            ByteBuffer buffer = ByteBuffer.allocate(FILE_SIZE);
            buffer.putLong(TERM_OFFSET, 0L);
            buffer.position(VOTED_FOR_OFFSET);
            buffer.put((byte) 0);

            try (FileChannel channel = FileChannel.open(stateFile, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
                buffer.flip();
                channel.write(buffer);
                channel.force(true);
            }
            log.info("[Persistence] Created new state file");
        }
    }

    @Override
    public void saveTerm(long term) throws IOException {
        lock.writeLock().lock();
        try {
            ByteBuffer buffer = ByteBuffer.allocate(8);
            buffer.putLong(term);
            buffer.flip();

            try (FileChannel channel = FileChannel.open(stateFile, StandardOpenOption.WRITE)) {
                channel.position(TERM_OFFSET);
                channel.write(buffer);
                channel.force(true);
            }
            log.debug("[Persistence] Saved term={}", term);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public long loadTerm() throws IOException {
        lock.readLock().lock();
        try {
            ByteBuffer buffer = ByteBuffer.allocate(8);
            try (FileChannel channel = FileChannel.open(stateFile, StandardOpenOption.READ)) {
                channel.position(TERM_OFFSET);
                int bytesRead = readFully(channel, buffer);
                if (bytesRead < 8) throw new IOException("Corrupted state file: cannot read term");
                buffer.flip();
                return buffer.getLong();
            }
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void saveVotedFor(String nodeId) throws IOException {
        lock.writeLock().lock();
        try {
            byte[] nodeIdBytes;
            if (nodeId == null || nodeId.isEmpty()) {
                nodeIdBytes = new byte[]{0};
            } else {
                byte[] encoded = nodeId.getBytes(StandardCharsets.UTF_8);
                if (encoded.length > VOTED_FOR_MAX_LENGTH) throw new IOException("VotedFor nodeId too long: " + encoded.length);
                nodeIdBytes = new byte[encoded.length + 1];
                nodeIdBytes[0] = (byte) encoded.length;
                System.arraycopy(encoded, 0, nodeIdBytes, 1, encoded.length);
            }

            ByteBuffer buffer = ByteBuffer.allocate(VOTED_FOR_MAX_LENGTH + 1);
            buffer.put(nodeIdBytes);
            buffer.flip();

            try (FileChannel channel = FileChannel.open(stateFile, StandardOpenOption.WRITE)) {
                channel.position(VOTED_FOR_OFFSET);
                channel.write(buffer);
                channel.force(true);
            }
            log.debug("[Persistence] Saved votedFor={}", nodeId);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public String loadVotedFor() throws IOException {
        lock.readLock().lock();
        try {
            ByteBuffer buffer = ByteBuffer.allocate(VOTED_FOR_MAX_LENGTH + 1);
            try (FileChannel channel = FileChannel.open(stateFile, StandardOpenOption.READ)) {
                channel.position(VOTED_FOR_OFFSET);
                int bytesRead = readFully(channel, buffer);
                if (bytesRead < 1) throw new IOException("Corrupted state file: cannot read votedFor");
                buffer.flip();
                int length = buffer.get() & 0xFF;
                if (length == 0) return null;
                byte[] nodeIdBytes = new byte[length];
                buffer.get(nodeIdBytes);
                return new String(nodeIdBytes, StandardCharsets.UTF_8);
            }
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void saveBoth(long term, String votedFor) throws IOException {
        lock.writeLock().lock();
        try {
            ByteBuffer buffer = ByteBuffer.allocate(FILE_SIZE);
            buffer.putLong(TERM_OFFSET, term);
            buffer.position(VOTED_FOR_OFFSET);
            if (votedFor == null || votedFor.isEmpty()) {
                buffer.put((byte) 0);
            } else {
                byte[] encoded = votedFor.getBytes(StandardCharsets.UTF_8);
                if (encoded.length > VOTED_FOR_MAX_LENGTH) throw new IOException("VotedFor nodeId too long: " + encoded.length);
                buffer.put((byte) encoded.length);
                buffer.put(encoded);
            }

            buffer.flip();
            try (FileChannel channel = FileChannel.open(stateFile, StandardOpenOption.WRITE)) {
                channel.position(0);
                channel.write(buffer);
                channel.force(true);
            }
            log.debug("[Persistence] Saved term={} votedFor={}", term, votedFor);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void appendLogEntry(LogEntry entry) throws IOException {
        lock.writeLock().lock();
        try (FileChannel channel = FileChannel.open(logFile,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.APPEND)) {

            ByteBuffer buffer = serializeLogEntry(entry);
            channel.write(buffer);
            channel.force(true);
            log.debug("[Persistence] Appended log entry index={} term={}", entry.index(), entry.term());
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void appendLogEntries(List<LogEntry> entries) throws IOException {
        if (entries.isEmpty()) return;

        lock.writeLock().lock();
        try (FileChannel channel = FileChannel.open(logFile,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.APPEND)) {

            for (LogEntry entry : entries) {
                ByteBuffer buffer = serializeLogEntry(entry);
                channel.write(buffer);
            }
            channel.force(true);
            log.debug("[Persistence] Appended {} log entries", entries.size());
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public List<LogEntry> loadLog() throws IOException {
        lock.readLock().lock();
        try {
            return loadLogUnlocked();
        } finally {
            lock.readLock().unlock();
        }
    }

    private List<LogEntry> loadLogUnlocked() throws IOException {
        if (!Files.exists(logFile) || Files.size(logFile) == 0) {
            log.info("[Persistence] No log file found or empty, starting with empty log");
            return new ArrayList<>();
        }

        List<LogEntry> entries = new ArrayList<>();
        try (FileChannel channel = FileChannel.open(logFile, StandardOpenOption.READ)) {
            while (channel.position() < channel.size()) {
                try {
                    LogEntry entry = deserializeLogEntry(channel);
                    if (entry != null) entries.add(entry);
                } catch (Exception e) {
                    log.error("[Persistence] Failed to deserialize log entry at position {}, stopping load", channel.position(), e);
                    break;
                }
            }
        }
        log.info("[Persistence] Loaded {} log entries from disk", entries.size());
        return entries;
    }

    @Override
    public void truncateLog(long fromIndex) throws IOException {
        lock.writeLock().lock();
        try {
            List<LogEntry> allEntries = loadLogUnlocked();
            if (fromIndex <= 1 || allEntries.isEmpty()) {
                Files.deleteIfExists(logFile);
                log.debug("Log truncated completely (fromIndex <= 1)");
                return;
            }

            List<LogEntry> keep = new ArrayList<>();
            for (LogEntry e : allEntries) {
                if (e.index() < fromIndex) {
                    keep.add(e);
                }
            }

            Path tmp = logFile.resolveSibling(logFile.getFileName() + ".tmp");
            try (FileChannel ch = FileChannel.open(tmp,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.TRUNCATE_EXISTING)) {
                for (LogEntry e : keep) {
                    ch.write(serializeLogEntry(e));
                }
                ch.force(true);
            }
            Files.move(tmp, logFile, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            log.info("Log truncated: kept entries from index {} ({} entries kept, {} removed)", fromIndex, keep.size(), allEntries.size() - keep.size());
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public long getLogSize() throws IOException {
        lock.readLock().lock();
        try {
            if (!Files.exists(logFile)) return 0;
            List<LogEntry> entries = loadLog();
            return entries.size();
        } finally {
            lock.readLock().unlock();
        }
    }

    private ByteBuffer serializeLogEntry(LogEntry entry) throws IOException {
        Command cmd = entry.command();
        byte[] keyBytes = cmd.key().getBytes(StandardCharsets.UTF_8);
        if (keyBytes.length > MAX_KEY_LENGTH) throw new IOException("Key too long: " + keyBytes.length);

        byte[] valueBytes = null;
        if (cmd.value() != null) {
            valueBytes = cmd.value().getBytes(StandardCharsets.UTF_8);
            if (valueBytes.length > MAX_VALUE_LENGTH) throw new IOException("Value too long: " + valueBytes.length);
        }
        int size = 8 + 8 + 1 + 4 + keyBytes.length + 4 + (valueBytes != null ? valueBytes.length : 0);
        ByteBuffer buffer = ByteBuffer.allocate(size);

        buffer.putLong(entry.index());
        buffer.putLong(entry.term());
        buffer.put((byte) cmd.type().ordinal());
        buffer.putInt(keyBytes.length);
        buffer.put(keyBytes);
        buffer.putInt(valueBytes != null ? valueBytes.length : -1);
        if (valueBytes != null) buffer.put(valueBytes);
        buffer.flip();
        return buffer;
    }

    private LogEntry deserializeLogEntry(FileChannel channel) throws IOException {
        ByteBuffer header = ByteBuffer.allocate(8 + 8 + 1);
        int read = readFully(channel, header);
        if (read < header.capacity()) return null;
        header.flip();
        long index = header.getLong();
        long term = header.getLong();
        byte typeOrdinal = header.get();
        if (typeOrdinal < 0 || typeOrdinal >= Command.Type.values().length) throw new IOException("Invalid command type ordinal: " + typeOrdinal);
        ByteBuffer keyLenBuf = ByteBuffer.allocate(4);
        if (readFully(channel, keyLenBuf) < 4) throw new IOException("Incomplete key length");
        keyLenBuf.flip();
        int keyLen = keyLenBuf.getInt();
        if (keyLen < 0 || keyLen > MAX_KEY_LENGTH) throw new IOException("Invalid key length: " + keyLen);
        ByteBuffer keyBuf = ByteBuffer.allocate(keyLen);
        if (readFully(channel, keyBuf) < keyLen) throw new IOException("Incomplete key data");
        keyBuf.flip();
        String key = StandardCharsets.UTF_8.decode(keyBuf).toString();
        ByteBuffer valueLenBuf = ByteBuffer.allocate(4);
        if (readFully(channel, valueLenBuf) < 4) throw new IOException("Incomplete value length");
        valueLenBuf.flip();
        int valueLen = valueLenBuf.getInt();
        String value = null;
        if (valueLen >= 0) {
            if (valueLen > MAX_VALUE_LENGTH) throw new IOException("Invalid value length: " + valueLen);
            ByteBuffer valueBuf = ByteBuffer.allocate(valueLen);
            if (readFully(channel, valueBuf) < valueLen) throw new IOException("Incomplete value data");
            valueBuf.flip();
            value = StandardCharsets.UTF_8.decode(valueBuf).toString();
        }

        Command.Type type = Command.Type.values()[typeOrdinal];
        Command cmd = new Command(type, key, value);
        return new LogEntry(index, term, cmd);
    }

    private static int readFully(FileChannel channel, ByteBuffer buf) throws IOException {
        int total = 0;
        while (buf.hasRemaining()) {
            int r = channel.read(buf);
            if (r < 0) break;
            total += r;
        }
        return total;
    }

    @Override
    public void saveSnapshot(long lastIncludedIndex, long lastIncludedTerm, Map<String, String> data) throws IOException {
        lock.writeLock().lock();
        try {
            Path tmp = snapshotFile.resolveSibling(snapshotFile.getFileName() + ".tmp");

            try (FileChannel channel = FileChannel.open(tmp,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.TRUNCATE_EXISTING)) {

                ByteBuffer header = ByteBuffer.allocate(8 + 8 + 4);
                header.putLong(lastIncludedIndex);
                header.putLong(lastIncludedTerm);
                header.putInt(data.size());
                header.flip();
                channel.write(header);

                for (Map.Entry<String, String> entry : data.entrySet()) {
                    byte[] keyBytes = entry.getKey().getBytes(StandardCharsets.UTF_8);
                    byte[] valueBytes = entry.getValue().getBytes(StandardCharsets.UTF_8);

                    if (keyBytes.length > MAX_KEY_LENGTH) {
                        log.warn("[Snapshot] Skipping key longer than max: {} bytes", keyBytes.length);
                        continue;
                    }
                    if (valueBytes.length > MAX_VALUE_LENGTH) {
                        log.warn("[Snapshot] Skipping value longer than max: {} bytes", valueBytes.length);
                        continue;
                    }

                    ByteBuffer entryBuf = ByteBuffer.allocate(4 + keyBytes.length + 4 + valueBytes.length);
                    entryBuf.putInt(keyBytes.length);
                    entryBuf.put(keyBytes);
                    entryBuf.putInt(valueBytes.length);
                    entryBuf.put(valueBytes);
                    entryBuf.flip();
                    channel.write(entryBuf);
                }

                channel.force(true);
            }

            Files.move(tmp, snapshotFile, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            log.info("[Snapshot] Saved snapshot at index={} term={} with {} entries",
                lastIncludedIndex, lastIncludedTerm, data.size());
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public Map<String, String> loadSnapshotData() throws IOException {
        lock.readLock().lock();
        try {
            if (!Files.exists(snapshotFile)) return new HashMap<>();

            Map<String, String> data = new HashMap<>();
            try (FileChannel channel = FileChannel.open(snapshotFile, StandardOpenOption.READ)) {
                ByteBuffer header = ByteBuffer.allocate(8 + 8 + 4);
                if (readFully(channel, header) < header.capacity()) {
                    log.warn("[Snapshot] Corrupted snapshot file header");
                    return new HashMap<>();
                }
                header.flip();

                long lastIncludedIndex = header.getLong();
                long lastIncludedTerm = header.getLong();
                int entryCount = header.getInt();

                log.info("[Snapshot] Loading snapshot: index={} term={} entries={}", lastIncludedIndex, lastIncludedTerm, entryCount);

                for (int i = 0; i < entryCount; i++) {
                    ByteBuffer keyLenBuf = ByteBuffer.allocate(4);
                    if (readFully(channel, keyLenBuf) < 4) break;
                    keyLenBuf.flip();
                    int keyLen = keyLenBuf.getInt();

                    if (keyLen <= 0 || keyLen > MAX_KEY_LENGTH) {
                        log.warn("[Snapshot] Invalid key length: {}", keyLen);
                        break;
                    }

                    ByteBuffer keyBuf = ByteBuffer.allocate(keyLen);
                    if (readFully(channel, keyBuf) < keyLen) break;
                    keyBuf.flip();
                    String key = StandardCharsets.UTF_8.decode(keyBuf).toString();

                    ByteBuffer valueLenBuf = ByteBuffer.allocate(4);
                    if (readFully(channel, valueLenBuf) < 4) break;
                    valueLenBuf.flip();
                    int valueLen = valueLenBuf.getInt();

                    if (valueLen < 0 || valueLen > MAX_VALUE_LENGTH) {
                        log.warn("[Snapshot] Invalid value length: {}", valueLen);
                        break;
                    }

                    ByteBuffer valueBuf = ByteBuffer.allocate(valueLen);
                    if (readFully(channel, valueBuf) < valueLen) break;
                    valueBuf.flip();
                    String value = StandardCharsets.UTF_8.decode(valueBuf).toString();

                    data.put(key, value);
                }
            }

            log.info("[Snapshot] Loaded {} entries from snapshot", data.size());
            return data;
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public long getSnapshotLastIncludedIndex() throws IOException {
        lock.readLock().lock();
        try {
            if (!Files.exists(snapshotFile)) {
                return 0;
            }

            try (FileChannel channel = FileChannel.open(snapshotFile, StandardOpenOption.READ)) {
                ByteBuffer buf = ByteBuffer.allocate(8);
                if (readFully(channel, buf) < 8) return 0;
                buf.flip();
                return buf.getLong();
            }
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public long getSnapshotLastIncludedTerm() throws IOException {
        lock.readLock().lock();
        try {
            if (!Files.exists(snapshotFile)) {
                return 0;
            }

            try (FileChannel channel = FileChannel.open(snapshotFile, StandardOpenOption.READ)) {
                ByteBuffer buf = ByteBuffer.allocate(16);
                if (readFully(channel, buf) < 16) return 0;
                buf.flip();
                buf.getLong(); // Skip lastIncludedIndex
                return buf.getLong();
            }
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public boolean hasSnapshot() throws IOException {
        return Files.exists(snapshotFile) && Files.size(snapshotFile) > 0;
    }

    @Override
    public void deleteOldSnapshots() throws IOException {
        log.debug("[Snapshot] Delete old snapshots (no-op for now)");
    }
}