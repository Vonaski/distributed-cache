package com.iksanov.distributedcache.node.consensus.raft;

import com.iksanov.distributedcache.node.core.CacheStore;
import com.iksanov.distributedcache.node.consensus.model.Command;
import com.iksanov.distributedcache.node.consensus.model.LogEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * Default RaftStateMachine implementation that applies LogEntry -> Command to a provided CacheStore.
 * <p>
 * - Designed to be called from RaftNode's single-threaded executor in commit order.
 * - Uses the provided CacheStore (e.g. InMemoryCacheStore) as the authoritative backing store.
 * - Keeps track of lastAppliedIndex for observability and testing.
 * - Supports an optional applyCallback for additional side-effects (metrics, persistence, replication hooks).
 * <p>
 * Notes:
 * - RaftNode will call apply(...) from
 *   its single-threaded executor, but reads (get) might happen from other threads (HTTP handlers etc.).
 * - Snapshot/restore: this class exposes restoreSnapshot(Map) so RaftNode (or snapshot manager) can restore state.
 *   To create snapshots you need to provide a Map snapshot externally (CacheStore does not expose a full-scan API
 *   in the interface; if you want snapshotting built-in, extend CacheStore to add exportSnapshot()).
 */
public class DefaultRaftStateMachine implements RaftStateMachine {

    private static final Logger logger = LoggerFactory.getLogger(DefaultRaftStateMachine.class);
    private final CacheStore store;
    private final Consumer<LogEntry> applyCallback;
    private final AtomicLong lastAppliedIndex = new AtomicLong(0);

    public DefaultRaftStateMachine(CacheStore store) {
        this(store, null);
    }

    public DefaultRaftStateMachine(CacheStore store, Consumer<LogEntry> applyCallback) {
        this.store = Objects.requireNonNull(store, "store");
        this.applyCallback = applyCallback;
    }

    @Override
    public void apply(LogEntry entry) {
        Objects.requireNonNull(entry, "entry");
        if (entry.command() == null) {
            logger.warn("Applying entry {} with null command — skipping", entry.index());
            lastAppliedIndex.set(entry.index());
            return;
        }

        Command cmd = entry.command();
        try {
            switch (cmd.type()) {
                case SET -> {
                    store.put(cmd.key(), cmd.value());
                    logger.debug("Applied SET key='{}' index={}", cmd.key(), entry.index());
                }
                case DELETE -> {
                    store.delete(cmd.key());
                    logger.debug("Applied DELETE key='{}' index={}", cmd.key(), entry.index());
                }
                default -> logger.warn("Unknown command type {} in entry {}", cmd.type(), entry);
            }
            lastAppliedIndex.set(entry.index());

            if (applyCallback != null) {
                try {
                    applyCallback.accept(entry);
                } catch (Throwable t) {
                    logger.error("applyCallback threw for entry {}: {}", entry.index(), t.getMessage(), t);
                }
            }
        } catch (Throwable t) {
            logger.error("Failed to apply entry {}: {}", entry.index(), t.getMessage(), t);
            throw t;
        }
    }

    public long getLastAppliedIndex() {
        return lastAppliedIndex.get();
    }

    public String get(String key) {
        return store.get(key);
    }

    public int size() {
        return store.size();
    }

    public void restoreSnapshot(Map<String, String> snapshot) {
        Objects.requireNonNull(snapshot, "snapshot");
        store.clear();
        for (Map.Entry<String, String> e : snapshot.entrySet()) {
            store.put(e.getKey(), e.getValue());
        }
        logger.info("State machine restored snapshot with {} keys", snapshot.size());
    }
}
