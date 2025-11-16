package com.iksanov.distributedcache.node.consensus.raft;

import com.iksanov.distributedcache.node.consensus.model.AppendEntriesRequest;
import com.iksanov.distributedcache.node.consensus.model.AppendEntriesResponse;
import com.iksanov.distributedcache.node.consensus.model.Command;
import com.iksanov.distributedcache.node.consensus.model.LogEntry;
import com.iksanov.distributedcache.node.consensus.model.VoteRequest;
import com.iksanov.distributedcache.node.consensus.model.VoteResponse;
import com.iksanov.distributedcache.node.consensus.persistence.RaftPersistence;
import com.iksanov.distributedcache.node.consensus.transport.MessageType;
import com.iksanov.distributedcache.node.consensus.transport.RaftTransport;
import com.iksanov.distributedcache.node.consensus.transport.TransportMessage;
import com.iksanov.distributedcache.node.metrics.RaftMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Raft consensus implementation with full persistence support.
 * <p>
 * Key features:
 * - Log persistence with crash recovery
 * - Correct quorum calculations
 * - Graceful shutdown
 * - Memory leak prevention
 * - Comprehensive error handling
 * - Exponential backoff for retries
 */
public class RaftNode {
    private static final Logger log = LoggerFactory.getLogger(RaftNode.class);
    private final String nodeId;
    private final List<String> peers;
    private final RaftTransport transport;
    private final RaftStateMachine stateMachine;
    private final RaftPersistence persistence;
    private final RaftMetrics metrics;
    private final int minElectionMs;
    private final int maxElectionMs;
    private final int heartbeatMs;
    private final int submitTimeoutSeconds;
    private final int maxBatchSize;
    private final int maxBatchDelayMs;
    private final ScheduledExecutorService scheduler;
    private final ExecutorService exec;
    private volatile long currentTerm;
    private volatile String votedFor;
    private volatile RaftRole role = RaftRole.FOLLOWER;
    private volatile String leaderId = null;
    private volatile long commitIndex = 0;
    private volatile long lastApplied = 0;
    private final List<LogEntry> logStore = new ArrayList<>();
    private final Map<String, Long> nextIndex = new ConcurrentHashMap<>();
    private final Map<String, Long> matchIndex = new ConcurrentHashMap<>();
    private final Map<Long, CompletableFuture<Long>> outstanding = new ConcurrentHashMap<>();
    private ScheduledFuture<?> electionFuture;
    private ScheduledFuture<?> heartbeatFuture;
    private ScheduledFuture<?> flushFuture;
    private final AtomicLong corrIdGen = new AtomicLong(1);
    private int votesGranted;
    private volatile boolean persistenceFailed = false;
    private final List<PendingEntry> pendingBatch = new ArrayList<>();
    private static final int MAX_ENTRIES = 10_000;
    private static final int DEFAULT_SUBMIT_TIMEOUT_SECONDS = 30;
    private static final int SNAPSHOT_INTERVAL = 10_000;
    private volatile long lastSnapshotIndex = 0;

    /**
     * Constructor with full validation and crash recovery.
     *
     * @param nodeId unique node identifier
     * @param peers list of peer addresses ("host:port")
     * @param transport network transport implementation
     * @param stateMachine state machine for applying commands
     * @param persistence durable storage for Raft state and log
     * @param metrics metrics collector
     * @param minElectionMs minimum election timeout (ms)
     * @param maxElectionMs maximum election timeout (ms)
     * @param heartbeatMs heartbeat interval (ms)
     * @param maxBatchSize maximum number of entries to batch before flushing
     * @param maxBatchDelayMs maximum delay before flushing batch (ms)
     */
    public RaftNode(String nodeId, List<String> peers, RaftTransport transport,
                    RaftStateMachine stateMachine, RaftPersistence persistence, RaftMetrics metrics,
                    int minElectionMs, int maxElectionMs, int heartbeatMs, int maxBatchSize, int maxBatchDelayMs) {

        this.nodeId = Objects.requireNonNull(nodeId, "nodeId cannot be null");
        Objects.requireNonNull(peers, "peers cannot be null");

        if (peers.isEmpty()) throw new IllegalArgumentException("peers list cannot be empty");
        if (peers.size() < 2) log.warn("{} cluster size is {}, minimum 3 nodes recommended for fault tolerance", nodeId, peers.size() + 1);
        if (minElectionMs <= 0 || maxElectionMs <= minElectionMs) throw new IllegalArgumentException(String.format("Invalid election timeouts: min=%d max=%d", minElectionMs, maxElectionMs));
        if (heartbeatMs <= 0 || heartbeatMs >= minElectionMs) throw new IllegalArgumentException(String.format("heartbeatMs (%d) must be less than minElectionMs (%d)", heartbeatMs, minElectionMs));

        this.peers = List.copyOf(peers);
        this.transport = Objects.requireNonNull(transport, "transport cannot be null");
        this.stateMachine = Objects.requireNonNull(stateMachine, "stateMachine cannot be null");
        this.persistence = Objects.requireNonNull(persistence, "persistence cannot be null");
        this.metrics = Objects.requireNonNull(metrics, "metrics cannot be null");
        this.minElectionMs = minElectionMs;
        this.maxElectionMs = maxElectionMs;
        this.heartbeatMs = heartbeatMs;
        this.maxBatchSize = maxBatchSize;
        this.maxBatchDelayMs = maxBatchDelayMs;
        this.submitTimeoutSeconds = DEFAULT_SUBMIT_TIMEOUT_SECONDS;

        this.exec = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "raft-main-" + nodeId);
            t.setUncaughtExceptionHandler((thread, throwable) -> log.error("{} uncaught exception in raft executor", nodeId, throwable));
            return t;
        });
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> new Thread(r, "raft-scheduler-" + nodeId));
        log.info("{} initializing with cluster config: total_nodes={}, quorum={}, election_timeout=[{}-{}]ms, heartbeat={}ms, batch={}/{} ms", nodeId, peers.size() + 1, quorumSize(), minElectionMs, maxElectionMs, heartbeatMs, maxBatchSize, maxBatchDelayMs);
        recoverPersistedState();
        transport.registerHandler(MessageType.APPEND_ENTRIES, this::handleTransportMessage);
        transport.registerHandler(MessageType.APPEND_ENTRIES_RESPONSE, this::handleTransportMessage);
        transport.registerHandler(MessageType.REQUEST_VOTE, this::handleTransportMessage);
        transport.registerHandler(MessageType.REQUEST_VOTE_RESPONSE, this::handleTransportMessage);
        scheduleElectionTimeout();
        log.info("{} initialization complete, starting as FOLLOWER", nodeId);
    }

    private void recoverPersistedState() {
        try {
            this.currentTerm = persistence.loadTerm();
            this.votedFor = persistence.loadVotedFor();
            log.info("{} recovered metadata: term={}, votedFor={}", nodeId, currentTerm, votedFor);
        } catch (Exception e) {
            log.warn("{} failed to load persisted metadata, starting fresh with term=0", nodeId, e);
            this.currentTerm = 0;
            this.votedFor = null;
        }
        metrics.setCurrentTerm(currentTerm);

        try {
            if (persistence.hasSnapshot()) {
                long snapshotIndex = persistence.getSnapshotLastIncludedIndex();
                long snapshotTerm = persistence.getSnapshotLastIncludedTerm();
                Map<String, String> snapshotData = persistence.loadSnapshotData();

                if (!snapshotData.isEmpty()) {
                    log.info("{} loading snapshot: index={}, term={}, entries={}",
                        nodeId, snapshotIndex, snapshotTerm, snapshotData.size());

                    // Restore state machine from snapshot
                    stateMachine.restoreSnapshot(snapshotData);
                    lastSnapshotIndex = snapshotIndex;
                    lastApplied = snapshotIndex;
                    commitIndex = snapshotIndex;

                    log.info("{} restored state from snapshot (lastApplied={})", nodeId, lastApplied);
                }
            }
        } catch (Exception e) {
            log.warn("{} failed to load snapshot, continuing with log recovery", nodeId, e);
        }

        try {
            List<LogEntry> persistedLog = persistence.loadLog();

            List<LogEntry> remainingLog = new ArrayList<>();
            for (LogEntry entry : persistedLog) {
                if (entry.index() > lastSnapshotIndex) {
                    remainingLog.add(entry);
                }
            }

            logStore.addAll(remainingLog);
            metrics.setLogSize(logStore.size());

            if (!remainingLog.isEmpty()) {
                long firstIndex = remainingLog.getFirst().index();
                long lastIndex = remainingLog.getLast().index();
                log.info("{} recovered log: {} entries [index {}-{}] (after snapshot at {})",
                    nodeId, remainingLog.size(), firstIndex, lastIndex, lastSnapshotIndex);

                if (lastApplied < logStore.size()) {
                    log.info("{} applying {} entries after snapshot", nodeId, logStore.size() - lastApplied);
                    applyEntriesUpTo(logStore.size());
                }
            } else {
                log.info("{} recovered log: empty (snapshot at index={})", nodeId, lastSnapshotIndex);
            }
        } catch (Exception e) {
            log.error("{} failed to recover log from disk, starting with empty log", nodeId, e);
        }

        metrics.setCommitIndex(commitIndex);
        metrics.setNodeState(role.ordinal());
    }

    private int quorumSize() {
        int totalNodes = peers.size() + 1;
        return totalNodes / 2 + 1;
    }

    public void start() {
        try {
            transport.start();
            log.info("{} transport started, node is operational", nodeId);
        } catch (Exception e) {
            log.error("{} failed to start transport", nodeId, e);
            throw new RuntimeException("Failed to start Raft node", e);
        }
    }

    public void stop() {
        log.info("{} initiating graceful shutdown", nodeId);

        if (!pendingBatch.isEmpty()) {
            log.info("{} flushing {} pending entries before shutdown", nodeId, pendingBatch.size());
            exec.execute(this::flushBatch);
        }

        try {
            if (electionFuture != null) {
                electionFuture.cancel(false);
                electionFuture = null;
            }
            if (heartbeatFuture != null) {
                heartbeatFuture.cancel(false);
                heartbeatFuture = null;
            }
            if (flushFuture != null) {
                flushFuture.cancel(false);
                flushFuture = null;
            }
            scheduler.shutdown();
            exec.shutdown();
            boolean execTerminated = exec.awaitTermination(5, TimeUnit.SECONDS);
            boolean schedulerTerminated = scheduler.awaitTermination(5, TimeUnit.SECONDS);
            if (!execTerminated) {
                log.warn("{} main executor did not terminate gracefully, forcing shutdown", nodeId);
                exec.shutdownNow();
            }
            if (!schedulerTerminated) {
                log.warn("{} scheduler did not terminate gracefully, forcing shutdown", nodeId);
                scheduler.shutdownNow();
            }

            transport.shutdown();
            log.info("{} shutdown complete", nodeId);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("{} shutdown interrupted, forcing immediate termination", nodeId);
            exec.shutdownNow();
            scheduler.shutdownNow();
        } catch (Exception e) {
            log.error("{} error during shutdown", nodeId, e);
        }
    }

    private void shutdownDueToPersistenceFailure(String operation, Exception cause) {
        if (persistenceFailed) return; // Already shutting down
        persistenceFailed = true;

        log.error("╔═══════════════════════════════════════════════════════════╗");
        log.error("║ CRITICAL PERSISTENCE FAILURE - NODE SHUTTING DOWN         ║");
        log.error("╠═══════════════════════════════════════════════════════════╣");
        log.error("║ Node: {}                                                  ║", nodeId);
        log.error("║ Operation: {}                                             ║", operation);
        log.error("║ Reason: Cannot maintain Raft durability guarantees        ║");
        log.error("║ Action: Initiating emergency shutdown                     ║");
        log.error("╚═══════════════════════════════════════════════════════════╝");

        for (Map.Entry<Long, CompletableFuture<Long>> entry : outstanding.entrySet()) {
            entry.getValue().completeExceptionally(
                new IOException("Node shutting down due to persistence failure: " + operation, cause)
            );
        }
        outstanding.clear();

        new Thread(() -> {
            try {
                Thread.sleep(100);
                stop();
                log.error("{} shutdown complete due to persistence failure", nodeId);
            } catch (Exception e) {
                log.error("{} error during emergency shutdown", nodeId, e);
            }
        }, "raft-emergency-shutdown-" + nodeId).start();
    }

    public CompletableFuture<Long> submit(Command cmd) {
        CompletableFuture<Long> future = new CompletableFuture<>();

        exec.execute(() -> {
            if (role != RaftRole.LEADER) {
                future.completeExceptionally(new IllegalStateException("Not leader (current leader: " + leaderId + ")"));
                return;
            }

            pendingBatch.add(new PendingEntry(cmd, future));
            log.trace("{} added command to batch (size={})", nodeId, pendingBatch.size());

            if (pendingBatch.size() >= maxBatchSize) {
                log.debug("{} batch size limit reached ({}), flushing immediately", nodeId, maxBatchSize);
                flushBatch();
            } else {
                scheduleBatchFlush();
            }
        });
        return future;
    }

    private void scheduleSubmitTimeout(long index, CompletableFuture<Long> future) {
        scheduler.schedule(() -> {
            CompletableFuture<Long> pending = outstanding.remove(index);
            if (pending != null && !pending.isDone()) {
                pending.completeExceptionally(new TimeoutException("Entry not committed within " + submitTimeoutSeconds + " seconds"));
                log.warn("{} submit timeout for index={}", nodeId, index);
            }
        }, submitTimeoutSeconds, TimeUnit.SECONDS);
    }

    public String getLeaderId() {
        return leaderId;
    }

    public RaftRole getRole() {
        return role;
    }

    public long getCurrentTerm() {
        return currentTerm;
    }

    private void handleTransportMessage(TransportMessage tm) {
        exec.execute(() -> {
            try {
                Object rpc = tm.payload();
                if (rpc == null) {
                    log.warn("{} received null payload type={} from={}", nodeId, tm.type(), tm.from());
                    return;
                }

                switch (tm.type()) {
                    case APPEND_ENTRIES -> handleAppendEntries(tm, (AppendEntriesRequest) rpc);
                    case APPEND_ENTRIES_RESPONSE -> handleAppendEntriesResponse(tm, (AppendEntriesResponse) rpc);
                    case REQUEST_VOTE -> handleRequestVote(tm, (VoteRequest) rpc);
                    case REQUEST_VOTE_RESPONSE -> handleRequestVoteResponse(tm, (VoteResponse) rpc);
                    default -> log.warn("{} unknown message type={}", nodeId, tm.type());
                }
            } catch (Exception ex) {
                log.error("{} error processing message type={} from={}", nodeId, tm.type(), tm.from(), ex);
            }
        });
    }

    private void handleAppendEntries(TransportMessage tm, AppendEntriesRequest req) {
        metrics.incrementAppendEntriesReceived();
        boolean isHeartbeat = req.entries() == null || req.entries().isEmpty();
        if (isHeartbeat) metrics.incrementHeartbeatsReceived();

        if (req.term() < currentTerm) {
            log.debug("{} rejected AppendEntries from {} (stale term {} < {})", nodeId, req.leaderId(), req.term(), currentTerm);
            sendAppendEntriesResponse(tm.from(), new AppendEntriesResponse(currentTerm, false, logStore.size()), tm.correlationId());
            return;
        }

        if (req.term() > currentTerm) {
            log.info("{} discovered higher term {} from {}, stepping down from term {}", nodeId, req.term(), req.leaderId(), currentTerm);
            currentTerm = req.term();
            votedFor = null;
            persistMetadata(currentTerm, null);
        }

        if (role != RaftRole.FOLLOWER) {
            stepDownToFollower(req.leaderId());
        } else {
            leaderId = req.leaderId();
            scheduleElectionTimeout();
        }

        long lastLogIndex = lastSnapshotIndex + logStore.size();

        if (req.prevLogIndex() > lastLogIndex) {
            log.debug("{} rejected AppendEntries: log too short (prevIndex={}, lastLogIndex={})",
                nodeId, req.prevLogIndex(), lastLogIndex);
            sendAppendEntriesResponse(tm.from(), new AppendEntriesResponse(currentTerm, false, lastLogIndex), tm.correlationId());
            return;
        }

        if (req.prevLogIndex() > 0) {
            long prevTerm = getTermAtIndex(req.prevLogIndex());
            if (prevTerm != req.prevLogTerm()) {
                log.debug("{} rejected AppendEntries: term mismatch at prevIndex={} (expected={}, got={})",
                    nodeId, req.prevLogIndex(), req.prevLogTerm(), prevTerm);
                truncateLogFrom(req.prevLogIndex());
                sendAppendEntriesResponse(tm.from(), new AppendEntriesResponse(currentTerm, false, lastSnapshotIndex + logStore.size()), tm.correlationId());
                return;
            }
        }

        List<LogEntry> newEntries = new ArrayList<>();
        if (req.entries() != null) {
            for (LogEntry entry : req.entries()) {
                long idx = entry.index();
                int logPos = indexToLogPos(idx);
                if (logPos >= 0 && logPos < logStore.size()) {
                    // Entry exists in our log
                    LogEntry existing = logStore.get(logPos);
                    if (existing.term() != entry.term()) {
                        log.debug("{} conflict at index={}, truncating", nodeId, idx);
                        truncateLogFrom(idx);
                        newEntries.add(entry);
                    }
                } else if (idx > lastLogIndex) {
                    newEntries.add(entry);
                }
            }
        }

        if (!newEntries.isEmpty()) {
            try {
                persistence.appendLogEntries(newEntries);
                logStore.addAll(newEntries);
                metrics.setLogSize(logStore.size());
                log.debug("{} appended {} entries", nodeId, newEntries.size());
            } catch (IOException e) {
                log.error("{} CRITICAL: failed to persist {} new entries from leader", nodeId, newEntries.size(), e);
                sendAppendEntriesResponse(tm.from(), new AppendEntriesResponse(currentTerm, false, logStore.size()), tm.correlationId());
                shutdownDueToPersistenceFailure("appendLogEntries (" + newEntries.size() + " entries)", e);
                return;
            }
        }

        if (req.leaderCommit() > commitIndex) {
            long newCommitIndex = Math.min(req.leaderCommit(), logStore.size());
            log.debug("{} updating commitIndex from {} to {}", nodeId, commitIndex, newCommitIndex);
            commitIndex = newCommitIndex;
            metrics.setCommitIndex(commitIndex);
            applyEntriesUpTo(commitIndex);
        }
        sendAppendEntriesResponse(tm.from(), new AppendEntriesResponse(currentTerm, true, logStore.size()), tm.correlationId());
    }

    private void truncateLogFrom(long fromIndex) {
        try {
            persistence.truncateLog(fromIndex);
            while (logStore.size() >= fromIndex) {
                logStore.removeLast();
            }
            metrics.setLogSize(logStore.size());
            log.debug("{} truncated log from index={}", nodeId, fromIndex);
        } catch (IOException e) {
            log.error("{} CRITICAL: failed to truncate log from index={}", nodeId, fromIndex, e);
            shutdownDueToPersistenceFailure("truncateLog from index=" + fromIndex, e);
        }
    }

    private void handleAppendEntriesResponse(TransportMessage tm, AppendEntriesResponse resp) {
        if (resp.term() > currentTerm) {
            log.info("{} discovered higher term {} from {}, stepping down", nodeId, resp.term(), tm.from());
            currentTerm = resp.term();
            persistMetadata(currentTerm, null);
            stepDownToFollower(null);
            return;
        }

        if (role != RaftRole.LEADER) return;
        String peer = tm.from();
        if (resp.success()) {
            long newMatchIndex = resp.matchIndex();
            matchIndex.put(peer, newMatchIndex);
            nextIndex.put(peer, newMatchIndex + 1);
            metrics.incrementAppendEntriesSuccess();
            log.debug("{} peer {} accepted, matchIndex={}", nodeId, peer, newMatchIndex);
            advanceCommitIndex();
        } else {
            metrics.incrementAppendEntriesFailed();
            long currentNext = nextIndex.getOrDefault(peer, (long) logStore.size());
            long newNext = Math.max(1L, currentNext - Math.max(1L, currentNext / 2));
            nextIndex.put(peer, newNext);
            log.debug("{} peer {} rejected, decrementing nextIndex {} -> {}", nodeId, peer, currentNext, newNext);
            replicateToPeer(peer);
        }
    }

    private void sendAppendEntriesResponse(String to, AppendEntriesResponse resp, long correlationId) {
        transport.send(new TransportMessage(MessageType.APPEND_ENTRIES_RESPONSE, nodeId, to, correlationId, resp));
    }

    private void handleRequestVote(TransportMessage tm, VoteRequest req) {
        log.debug("{} received VoteRequest from {} for term {}", nodeId, req.candidateId(), req.term());
        boolean grant = false;
        if (req.term() < currentTerm) {
            log.debug("{} denied vote to {} (stale term {} < {})", nodeId, req.candidateId(), req.term(), currentTerm);
        } else {
            if (req.term() > currentTerm) {
                log.info("{} discovered higher term {} from candidate {}, updating term", nodeId, req.term(), req.candidateId());
                currentTerm = req.term();
                votedFor = null;
                persistMetadata(currentTerm, null);
                if (role != RaftRole.FOLLOWER) stepDownToFollower(null);
            }

            if ((votedFor == null || votedFor.equals(req.candidateId())) && isLogUpToDate(req.lastLogIndex(), req.lastLogTerm())) {
                grant = true;
                votedFor = req.candidateId();
                persistVotedFor(votedFor);
                scheduleElectionTimeout();
                log.info("{} granted vote to {} for term {}", nodeId, req.candidateId(), currentTerm);
            } else {
                log.debug("{} denied vote to {} (votedFor={}, log not up-to-date)", nodeId, req.candidateId(), votedFor);
            }
        }
        if (grant) {
            metrics.incrementVoteGranted();
        } else {
            metrics.incrementVoteDenied();
        }
        VoteResponse resp = new VoteResponse(currentTerm, grant);
        transport.send(new TransportMessage(MessageType.REQUEST_VOTE_RESPONSE, nodeId, tm.from(), tm.correlationId(), resp));
    }

    private boolean isLogUpToDate(long candidateLastIndex, long candidateLastTerm) {
        long myLastIndex = logStore.size();
        long myLastTerm = myLastIndex > 0 ? logStore.get((int) myLastIndex - 1).term() : 0L;
        if (candidateLastTerm > myLastTerm) return true;
        return candidateLastTerm == myLastTerm && candidateLastIndex >= myLastIndex;
    }

    private void handleRequestVoteResponse(TransportMessage tm, VoteResponse resp) {
        if (resp.term() > currentTerm) {
            log.info("{} discovered higher term {} from {}, stepping down", nodeId, resp.term(), tm.from());
            currentTerm = resp.term();
            persistMetadata(currentTerm, null);
            stepDownToFollower(null);
            return;
        }

        if (role != RaftRole.CANDIDATE) return;

        if (resp.voteGranted()) {
            votesGranted++;
            log.debug("{} received vote from {}, total votes={}/{}", nodeId, tm.from(), votesGranted, quorumSize());
            if (votesGranted >= quorumSize()) becomeLeader();
        } else {
            log.debug("{} vote denied by {}", nodeId, tm.from());
        }
    }

    private void scheduleElectionTimeout() {
        if (electionFuture != null) electionFuture.cancel(false);
        int timeout = ThreadLocalRandom.current().nextInt(minElectionMs, maxElectionMs + 1);
        electionFuture = scheduler.schedule(() -> exec.execute(this::onElectionTimeout), timeout, TimeUnit.MILLISECONDS);
        log.trace("{} scheduled election timeout in {}ms", nodeId, timeout);
    }

    private void onElectionTimeout() {
        log.debug("{} election timeout fired, current role={}", nodeId, role);
        startElection();
    }


    private void startElection() {
        long newTerm = currentTerm + 1;
        currentTerm = newTerm;
        votedFor = nodeId;
        votesGranted = 1;
        persistMetadata(currentTerm, votedFor);
        role = RaftRole.CANDIDATE;
        metrics.setNodeState(role.ordinal());
        metrics.incrementElectionsStarted(currentTerm);
        log.info("{} started election for term {}", nodeId, currentTerm);
        long lastIndex = logStore.size();
        long lastTerm = lastIndex > 0 ? logStore.get((int) lastIndex - 1).term() : 0L;
        VoteRequest req = new VoteRequest(currentTerm, nodeId, lastIndex, lastTerm);
        long corrId = nextCorrelation();
        for (String peer : peers) {
            transport.send(new TransportMessage(MessageType.REQUEST_VOTE, nodeId, peer, corrId, req));
            log.debug("{} sent VoteRequest to {} for term {}", nodeId, peer, currentTerm);
        }
        scheduleElectionTimeout();
    }

    private void becomeLeader() {
        log.info("{} became LEADER for term {}", nodeId, currentTerm);
        role = RaftRole.LEADER;
        leaderId = nodeId;
        metrics.setNodeState(role.ordinal());
        metrics.incrementElectionsWon(currentTerm);

        if (electionFuture != null) {
            electionFuture.cancel(false);
            electionFuture = null;
        }

        long nextIndexValue = logStore.size() + 1L;
        for (String peer : peers) {
            nextIndex.put(peer, nextIndexValue);
            matchIndex.put(peer, 0L);
        }

        if (heartbeatFuture != null) {
            heartbeatFuture.cancel(false);
        }
        heartbeatFuture = scheduler.scheduleAtFixedRate(() -> exec.execute(this::sendHeartbeats), 0, heartbeatMs, TimeUnit.MILLISECONDS);
        log.debug("{} initialized leader state, sending heartbeats every {}ms", nodeId, heartbeatMs);
    }


    private void stepDownToFollower(String newLeader) {
        RaftRole oldRole = role;

        if (oldRole == RaftRole.LEADER) {
            log.info("{} stepping down from LEADER to FOLLOWER (new leader: {})", nodeId, newLeader);
            metrics.incrementStepDowns(currentTerm);

            if (heartbeatFuture != null) {
                heartbeatFuture.cancel(false);
                heartbeatFuture = null;
            }

            if (flushFuture != null) {
                flushFuture.cancel(false);
                flushFuture = null;
            }

            for (PendingEntry pe : pendingBatch) {
                pe.future.completeExceptionally(new IllegalStateException("Leadership lost, new leader: " + newLeader));
            }
            pendingBatch.clear();

            for (Map.Entry<Long, CompletableFuture<Long>> entry : outstanding.entrySet()) {
                entry.getValue().completeExceptionally(new IllegalStateException("Leadership lost, new leader: " + newLeader));
            }
            outstanding.clear();
        } else if (oldRole == RaftRole.CANDIDATE) {
            log.info("{} stepping down from CANDIDATE to FOLLOWER", nodeId);
            metrics.incrementElectionsFailed();
        }
        role = RaftRole.FOLLOWER;
        leaderId = newLeader;
        votedFor = null;
        persistVotedFor(null);
        metrics.setNodeState(role.ordinal());
        scheduleElectionTimeout();
    }


    private void sendHeartbeats() {
        if (role != RaftRole.LEADER) return;
        log.trace("{} sending heartbeats to {} peers", nodeId, peers.size());
        for (String peer : peers) {
            long next = nextIndex.getOrDefault(peer, logStore.size() + 1L);
            long prevIdx = next - 1;
            long prevTerm = prevIdx > 0 && prevIdx <= logStore.size() ? logStore.get((int) prevIdx - 1).term() : 0L;

            AppendEntriesRequest req = new AppendEntriesRequest(
                    currentTerm, nodeId,
                    prevIdx, prevTerm,
                    Collections.emptyList(),
                    commitIndex);
            transport.send(new TransportMessage(MessageType.APPEND_ENTRIES, nodeId, peer, nextCorrelation(), req));
            metrics.incrementHeartbeatsSent();
        }
    }

    private void replicateToAll() {
        if (role != RaftRole.LEADER) return;
        for (String peer : peers) {
            replicateToPeer(peer);
        }
    }

    private void replicateToPeer(String peer) {
        long next = nextIndex.getOrDefault(peer, logStore.size() + 1L);

        if (next <= lastSnapshotIndex) {
            log.warn("{} peer {} is too far behind (nextIndex={}, lastSnapshotIndex={}). " +
                     "Peer needs to be restarted to restore from snapshot.",
                     nodeId, peer, next, lastSnapshotIndex);
            nextIndex.put(peer, lastSnapshotIndex + 1);
            AppendEntriesRequest req = new AppendEntriesRequest(
                currentTerm, nodeId, lastSnapshotIndex,
                lastSnapshotIndex > 0 ? getSnapshotLastTerm() : 0L,
                Collections.emptyList(), commitIndex);
            transport.send(new TransportMessage(MessageType.APPEND_ENTRIES, nodeId, peer, nextCorrelation(), req));
            return;
        }

        long prevIdx = next - 1;
        long prevTerm = 0L;

        if (prevIdx > lastSnapshotIndex) {
            int logPos = (int) (prevIdx - lastSnapshotIndex - 1);
            if (logPos >= 0 && logPos < logStore.size()) {
                prevTerm = logStore.get(logPos).term();
            }
        } else if (prevIdx == lastSnapshotIndex) {
            prevTerm = getSnapshotLastTerm();
        }

        List<LogEntry> entries = entriesFrom(next);
        AppendEntriesRequest req = new AppendEntriesRequest(currentTerm, nodeId, prevIdx, prevTerm, entries, commitIndex);
        transport.send(new TransportMessage(MessageType.APPEND_ENTRIES, nodeId, peer, nextCorrelation(), req));
        metrics.incrementAppendEntriesSent();
        if (!entries.isEmpty()) log.debug("{} replicating {} entries to {} (nextIndex={})", nodeId, entries.size(), peer, next);
    }

    private List<LogEntry> entriesFrom(long fromIndex) {
        if (fromIndex <= lastSnapshotIndex) {
            return Collections.emptyList();
        }

        long firstLogIndex = lastSnapshotIndex + 1;
        int startIdx = (int) (fromIndex - firstLogIndex);

        if (startIdx < 0 || startIdx >= logStore.size()) {
            return Collections.emptyList();
        }

        return new ArrayList<>(logStore.subList(startIdx, logStore.size()));
    }

    private long getSnapshotLastTerm() {
        try {
            if (persistence.hasSnapshot()) {
                return persistence.getSnapshotLastIncludedTerm();
            }
        } catch (Exception e) {
            log.warn("{} failed to get snapshot last term", nodeId, e);
        }
        return 0L;
    }

    private int indexToLogPos(long absoluteIndex) {
        if (absoluteIndex <= lastSnapshotIndex) {
            return -1;
        }
        long pos = absoluteIndex - lastSnapshotIndex - 1;
        if (pos < 0 || pos >= logStore.size()) {
            return -1;
        }
        return (int) pos;
    }

    private long getTermAtIndex(long absoluteIndex) {
        if (absoluteIndex == 0) return 0L;
        if (absoluteIndex == lastSnapshotIndex) {
            return getSnapshotLastTerm();
        }
        int pos = indexToLogPos(absoluteIndex);
        if (pos >= 0 && pos < logStore.size()) {
            return logStore.get(pos).term();
        }
        return 0L;
    }

    private void advanceCommitIndex() {
        if (role != RaftRole.LEADER) return;

        long N = commitIndex + 1;
        while (N <= logStore.size()) {
            int replicatedCount = 1;
            for (String peer : peers) {
                long peerMatchIndex = matchIndex.getOrDefault(peer, 0L);
                if (peerMatchIndex >= N) replicatedCount++;
            }

            if (replicatedCount >= quorumSize()) {
                LogEntry entry = logStore.get((int) N - 1);
                if (entry.term() == currentTerm) {
                    log.debug("{} advancing commitIndex from {} to {} (replicated to {}/{})", nodeId, commitIndex, N, replicatedCount, peers.size() + 1);
                    commitIndex = N;
                    metrics.setCommitIndex(commitIndex);
                    applyEntriesUpTo(commitIndex);
                    CompletableFuture<Long> future = outstanding.remove(N);
                    if (future != null && !future.isDone()) {
                        future.complete(N);
                        metrics.incrementCommandsReplicated();
                    }
                    N++;
                    continue;
                }
            }
            break;
        }
    }

    private void applyEntriesUpTo(long targetIndex) {
        long currentApplied = lastApplied;
        while (currentApplied < targetIndex) {
            currentApplied++;
            LogEntry entry = logStore.get((int) currentApplied - 1);
            try {
                stateMachine.apply(entry);
                metrics.incrementCommandsApplied();
                lastApplied = currentApplied;
                metrics.setLastApplied(lastApplied);
                log.trace("{} applied entry index={} to state machine", nodeId, currentApplied);
            } catch (Exception e) {
                log.error("{} failed to apply entry index={} to state machine", nodeId, currentApplied, e);
                metrics.incrementCommandsFailed();
                lastApplied = currentApplied;
            }
        }
        maybeSnapshot();
    }

    private void persistMetadata(long term, String votedFor) {
        try {
            persistence.saveBoth(term, votedFor);
            metrics.setCurrentTerm(term);
        } catch (IOException e) {
            log.error("{} CRITICAL: failed to persist metadata (term={}, votedFor={})", nodeId, term, votedFor, e);
            shutdownDueToPersistenceFailure("saveBoth(term=" + term + ", votedFor=" + votedFor + ")", e);
        }
    }

    private void persistVotedFor(String votedFor) {
        try {
            persistence.saveVotedFor(votedFor);
        } catch (IOException e) {
            log.error("{} CRITICAL: failed to persist votedFor={}", nodeId, votedFor, e);
            shutdownDueToPersistenceFailure("saveVotedFor(" + votedFor + ")", e);
        }
    }

    private long nextCorrelation() {
        return corrIdGen.getAndIncrement();
    }


    private record PendingEntry(Command command, CompletableFuture<Long> future) {
    }

    private void flushBatch() {
        if (pendingBatch.isEmpty()) return;
        if (role != RaftRole.LEADER) {
            for (PendingEntry pe : pendingBatch) {
                pe.future.completeExceptionally(new IllegalStateException("Not leader (current leader: " + leaderId + ")"));
            }
            pendingBatch.clear();
            return;
        }

        List<PendingEntry> toFlush = new ArrayList<>(pendingBatch);
        pendingBatch.clear();

        if (flushFuture != null) {
            flushFuture.cancel(false);
            flushFuture = null;
        }

        List<LogEntry> entries = new ArrayList<>(toFlush.size());
        long startIdx = logStore.size() + 1;
        for (int i = 0; i < toFlush.size(); i++) {
            long idx = startIdx + i;
            LogEntry entry = new LogEntry(idx, currentTerm, toFlush.get(i).command);
            entries.add(entry);
        }

        try {
            persistence.appendLogEntries(entries);
            log.debug("{} batch persisted {} entries at indices {}-{}", nodeId, entries.size(), startIdx, startIdx + entries.size() - 1);
        } catch (IOException e) {
            log.error("{} CRITICAL: failed to persist batch of {} entries", nodeId, entries.size(), e);
            for (PendingEntry pe : toFlush) {
                pe.future.completeExceptionally(new IOException("Failed to persist entry", e));
            }
            metrics.incrementCommandsFailed();
            shutdownDueToPersistenceFailure("appendLogEntries batch of " + entries.size(), e);
            return;
        }

        for (int i = 0; i < entries.size(); i++) {
            LogEntry entry = entries.get(i);
            logStore.add(entry);
            outstanding.put(entry.index(), toFlush.get(i).future);
            scheduleSubmitTimeout(entry.index(), toFlush.get(i).future);
        }

        metrics.setLogSize(logStore.size());
        replicateToAll();
        log.debug("{} flushed batch of {} entries, total log size={}", nodeId, entries.size(), logStore.size());
    }

    private void scheduleBatchFlush() {
        if (flushFuture == null || flushFuture.isDone()) {
            flushFuture = scheduler.schedule(() -> exec.execute(this::flushBatch), maxBatchDelayMs, TimeUnit.MILLISECONDS);
        }
    }

    private void maybeSnapshot() {
        long entriesSinceSnapshot = lastApplied - lastSnapshotIndex;
        if (entriesSinceSnapshot < SNAPSHOT_INTERVAL) {
            return;
        }

        log.info("{} creating snapshot at lastApplied={} (entries since last snapshot: {})",
            nodeId, lastApplied, entriesSinceSnapshot);

        try {
            Map<String, String> snapshotData = stateMachine.createSnapshot();

            long lastIncludedIndex = lastApplied;
            long lastIncludedTerm = 0;
            if (lastIncludedIndex > 0 && lastIncludedIndex <= logStore.size()) {
                long firstLogIndex = lastSnapshotIndex + 1;
                int logPosition = (int) (lastIncludedIndex - firstLogIndex);
                if (logPosition >= 0 && logPosition < logStore.size()) {
                    lastIncludedTerm = logStore.get(logPosition).term();
                }
            }

            persistence.saveSnapshot(lastIncludedIndex, lastIncludedTerm, snapshotData);
            log.info("{} snapshot saved: index={}, term={}, entries={}",
                nodeId, lastIncludedIndex, lastIncludedTerm, snapshotData.size());

            int entriesToRemove = (int) (lastIncludedIndex - lastSnapshotIndex);
            if (entriesToRemove > 0 && entriesToRemove <= logStore.size()) {
                for (int i = 0; i < entriesToRemove; i++) {
                    logStore.removeFirst();
                }
                metrics.setLogSize(logStore.size());
                log.info("{} trimmed {} log entries after snapshot, {} entries remaining",
                    nodeId, entriesToRemove, logStore.size());
            }
            lastSnapshotIndex = lastIncludedIndex;
            persistence.deleteOldSnapshots();
        } catch (Exception e) {
            log.error("{} failed to create snapshot at lastApplied={}", nodeId, lastApplied, e);
        }
    }
}