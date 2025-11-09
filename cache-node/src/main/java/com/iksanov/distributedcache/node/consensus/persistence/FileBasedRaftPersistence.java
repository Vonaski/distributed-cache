package com.iksanov.distributedcache.node.consensus.persistence;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * File-based persistence for Raft state.
 * Thread-safe, crash-resistant implementation using fsync.
 * <p>
 * File format (fixed 512 bytes):
 * - Bytes 0-7: currentTerm (long, big-endian)
 * - Bytes 8-263: votedFor (UTF-8 string, max 255 chars)
 * - Bytes 264-511: reserved for future use
 */
public class FileBasedRaftPersistence implements RaftPersistence{
    private static final Logger log = LoggerFactory.getLogger(FileBasedRaftPersistence.class);
    private static final int FILE_SIZE = 512;
    private static final int TERM_OFFSET = 0;
    private static final int VOTED_FOR_OFFSET = 8;
    private static final int VOTED_FOR_MAX_LENGTH = 255;
    private final Path stateFile;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    public FileBasedRaftPersistence(Path dataDirectory, String nodeId) throws IOException {
        Files.createDirectories(dataDirectory);
        this.stateFile = dataDirectory.resolve("raft-" + nodeId + ".state");
        initializeFile();
        log.info("[Persistence] Initialized file-based persistence at {}", stateFile);
    }

    private void initializeFile() throws IOException {
        if (!Files.exists(stateFile)) {
            ByteBuffer buffer = ByteBuffer.allocate(FILE_SIZE);
            buffer.putLong(TERM_OFFSET, 0L);
            buffer.position(VOTED_FOR_OFFSET);
            buffer.put((byte) 0);

            try (FileChannel channel = FileChannel.open(stateFile,
                    StandardOpenOption.CREATE_NEW,
                    StandardOpenOption.WRITE)) {
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

            try (FileChannel channel = FileChannel.open(stateFile,
                    StandardOpenOption.WRITE)) {
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
            try (FileChannel channel = FileChannel.open(stateFile,
                    StandardOpenOption.READ)) {
                channel.position(TERM_OFFSET);
                int bytesRead = channel.read(buffer);
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

            try (FileChannel channel = FileChannel.open(stateFile,
                    StandardOpenOption.WRITE)) {
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
            try (FileChannel channel = FileChannel.open(stateFile,
                    StandardOpenOption.READ)) {
                channel.position(VOTED_FOR_OFFSET);
                int bytesRead = channel.read(buffer);
                if (bytesRead < 1) {
                    throw new IOException("Corrupted state file: cannot read votedFor");
                }
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
            try (FileChannel channel = FileChannel.open(stateFile,
                    StandardOpenOption.WRITE)) {
                channel.position(0);
                channel.write(buffer);
                channel.force(true);
            }
            log.debug("[Persistence] Saved term={} votedFor={}", term, votedFor);
        } finally {
            lock.writeLock().unlock();
        }
    }
}