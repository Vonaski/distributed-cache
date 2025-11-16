package com.iksanov.distributedcache.node.consensus.raft;

import com.iksanov.distributedcache.node.core.CacheStore;
import com.iksanov.distributedcache.node.consensus.model.Command;
import com.iksanov.distributedcache.node.consensus.model.LogEntry;
import com.iksanov.distributedcache.node.metrics.RaftMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

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

    private static final Logger log = LoggerFactory.getLogger(DefaultRaftStateMachine.class);
    private final CacheStore store;
    private final RaftMetrics metrics;
    private final AtomicLong lastAppliedIndex = new AtomicLong(0);

    public DefaultRaftStateMachine(CacheStore store, RaftMetrics metrics) {
        this.store = Objects.requireNonNull(store, "store");
        this.metrics = Objects.requireNonNull(metrics, "metrics");
        log.info("DefaultRaftStateMachine initialized for store={} with metrics enabled", store.getClass().getSimpleName());
    }

    @Override
    public void apply(LogEntry entry) {
        Objects.requireNonNull(entry, "entry");
        if (entry.command() == null) {
            log.warn("Applying entry {} with null command — skipping", entry.index());
            lastAppliedIndex.set(entry.index());
            return;
        }

        Command cmd = entry.command();
        try {
            switch (cmd.type()) {
                case SET -> {
                    store.put(cmd.key(), cmd.value());
                    log.debug("Applied SET key='{}' index={}", cmd.key(), entry.index());
                }
                case DELETE -> {
                    store.delete(cmd.key());
                    log.debug("Applied DELETE key='{}' index={}", cmd.key(), entry.index());
                }
                default -> log.warn("Unknown command type {} in entry {}", cmd.type(), entry);
            }
            lastAppliedIndex.set(entry.index());
            metrics.incrementCommandsApplied();
            metrics.setLastApplied(getLastAppliedIndex());
        } catch (Exception e) {
            metrics.incrementCommandsFailed();
            log.error("Failed to apply command type={} key={} index={} term={}: {}", cmd.type(), cmd.key(), entry.index(), entry.term(), e.getMessage(), e);
        }
    }

    @Override
    public String get(String key) {
        return store.get(key);
    }
    @Override
    public long getLastAppliedIndex() {
        return lastAppliedIndex.get();
    }
    @Override
    public int size() {
        return store.size();
    }

    @Override
    public Map<String, String> createSnapshot() {
        Map<String, String> snapshot = store.exportData();
        log.info("Created snapshot with {} entries at lastAppliedIndex={}", snapshot.size(), lastAppliedIndex.get());
        return snapshot;
    }

    @Override
    public void restoreSnapshot(Map<String, String> snapshotData) {
        Objects.requireNonNull(snapshotData, "snapshotData");
        store.importData(snapshotData);
        log.info("Restored snapshot with {} entries", snapshotData.size());
    }
}
