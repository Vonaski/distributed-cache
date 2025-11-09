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
import com.iksanov.distributedcache.node.consensus.transport.RaftMessageCodec;
import com.iksanov.distributedcache.node.metrics.RaftMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Clean RaftNode implementation that uses RaftMessageCodec for (de)serialization.
 * <p>
 * - Builds RPC objects (AppendEntriesRequest, VoteRequest, etc.)
 * - Encodes them to byte[] using an EmbeddedChannel(RaftMessageCodec) and sends via RaftTransport as TransportMessage
 * - Decodes incoming TransportMessage.payload byte[] via same codec to RPC objects and handles them
 * <p>
 * All mutable Raft state is changed inside single-thread executor 'exec'.
 */
public class RaftNode {
    private static final Logger log = LoggerFactory.getLogger(RaftNode.class);

    private final String nodeId;
    private final List<String> peers;
    private final RaftTransport transport;
    private final RaftStateMachine stateMachine;
    private final RaftPersistence persistence;
    private final RaftMetrics metrics;

    private final ScheduledExecutorService scheduler;
    private final ExecutorService exec;

    // persistent metadata
    private volatile long currentTerm;
    private volatile String votedFor;

    // volatile Raft state
    private volatile RaftRole role = RaftRole.FOLLOWER;
    private volatile String leaderId = null;
    private volatile long commitIndex = 0;
    private volatile long lastApplied = 0;

    // in-memory log (index starting at 1). TODO: persist log in RaftPersistence when needed.
    private final List<LogEntry> logStore = new ArrayList<>();

    // leader-only state
    private final Map<String, Long> nextIndex = new ConcurrentHashMap<>();
    private final Map<String, Long> matchIndex = new ConcurrentHashMap<>();

    // outstanding client submissions waiting for commit
    private final Map<Long, CompletableFuture<Long>> outstanding = new ConcurrentHashMap<>();

    // timers & scheduling
    private final int minElectionMs;
    private final int maxElectionMs;
    private final int heartbeatMs;
    private ScheduledFuture<?> electionFuture;
    private ScheduledFuture<?> heartbeatFuture;

    private final AtomicLong corrIdGen = new AtomicLong(1);
    private int votesGranted; // accessed in exec only

    // codec helper instance reuse is safe because EmbeddedChannel is not thread-safe;
    // we create ephemeral EmbeddedChannel per encode/decode call to avoid threading issues.
    // constants for safety: keep consistent with RaftMessageCodec
    private static final int MAX_ENTRIES = 10_000;

    /**
     * Constructor.
     *
     * @param nodeId node id (unique, used as transport 'from')
     * @param peers peer addresses ("host:port" strings)
     * @param transport transport implementation (NettyRaftTransport)
     * @param stateMachine state machine implementation (DefaultRaftStateMachine)
     * @param persistence persistence for metadata (FileBasedRaftPersistence)
     * @param metrics metrics collector (RaftMetrics)
     * @param minElectionMs minimum election timeout (ms)
     * @param maxElectionMs maximum election timeout (ms)
     * @param heartbeatMs heartbeat interval (ms)
     */
    public RaftNode(String nodeId,
                    List<String> peers,
                    RaftTransport transport,
                    RaftStateMachine stateMachine,
                    RaftPersistence persistence,
                    RaftMetrics metrics,
                    int minElectionMs,
                    int maxElectionMs,
                    int heartbeatMs) {
        this.nodeId = Objects.requireNonNull(nodeId);
        this.peers = List.copyOf(Objects.requireNonNull(peers));
        this.transport = Objects.requireNonNull(transport);
        this.stateMachine = Objects.requireNonNull(stateMachine);
        this.persistence = Objects.requireNonNull(persistence);
        this.metrics = Objects.requireNonNull(metrics);
        this.minElectionMs = minElectionMs;
        this.maxElectionMs = maxElectionMs;
        this.heartbeatMs = heartbeatMs;

        this.exec = Executors.newSingleThreadExecutor(r -> new Thread(r, "raft-" + nodeId));
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> new Thread(r, "raft-timer-" + nodeId));

        // load persisted metadata
        try {
            this.currentTerm = persistence.loadTerm();
            this.votedFor = persistence.loadVotedFor();
            metrics.setCurrentTerm(currentTerm);
            log.info("{} loaded persisted term={} votedFor={}", nodeId, currentTerm, votedFor);
        } catch (Exception e) {
            log.warn("{} failed to load persisted metadata, starting with term=0", nodeId, e);
            this.currentTerm = 0;
            this.votedFor = null;
        }

        // register transport handler: it always passes TransportMessage with payload byte[] (per NettyRaftTransport)
        transport.registerHandler(MessageType.APPEND_ENTRIES, this::handleTransportMessage);
        transport.registerHandler(MessageType.APPEND_ENTRIES_RESPONSE, this::handleTransportMessage);
        transport.registerHandler(MessageType.REQUEST_VOTE, this::handleTransportMessage);
        transport.registerHandler(MessageType.REQUEST_VOTE_RESPONSE, this::handleTransportMessage);

        scheduleElectionTimeout();
        log.info("{} initialized with {} peers", nodeId, peers.size());
    }

    public void start() {
        try {
            transport.start();
            log.info("{} transport started", nodeId);
        } catch (Exception e) {
            throw new RuntimeException("Failed to start transport", e);
        }
    }

    public void stop() {
        try {
            if (electionFuture != null) electionFuture.cancel(true);
            if (heartbeatFuture != null) heartbeatFuture.cancel(true);
            scheduler.shutdownNow();
            exec.shutdownNow();
            transport.shutdown();
        } catch (Exception e) {
            log.warn("{} stop error", nodeId, e);
        } finally {
            log.info("{} stopped", nodeId);
        }
    }

    // -------------------------
    // transport glue: incoming TransportMessage -> decode via RaftMessageCodec -> forward to appropriate handler
    // -------------------------
    private void handleTransportMessage(TransportMessage tm) {
        // transport may call this from Netty threads; offload to exec
        exec.execute(() -> {
            try {
                // decode payload bytes into RPC object using codec
                Object rpc = decodePayload(tm.payload());
                if (rpc == null) {
                    log.warn("{} decode returned null for type {}", nodeId, tm.type());
                    return;
                }

                switch (tm.type()) {
                    case APPEND_ENTRIES -> handleAppendEntries(tm, (AppendEntriesRequest) rpc);
                    case APPEND_ENTRIES_RESPONSE -> handleAppendEntriesResponse(tm, (AppendEntriesResponse) rpc);
                    case REQUEST_VOTE -> handleRequestVote(tm, (VoteRequest) rpc);
                    case REQUEST_VOTE_RESPONSE -> handleRequestVoteResponse(tm, (VoteResponse) rpc);
                    default -> log.warn("{} unknown message type {}", nodeId, tm.type());
                }
            } catch (Exception ex) {
                log.error("{} failed to process transport message from {} type={}", nodeId, tm.from(), tm.type(), ex);
            }
        });
    }

    // -------------------------
    // client API
    // -------------------------
    public CompletableFuture<Long> submit(Command cmd) {
        CompletableFuture<Long> fut = new CompletableFuture<>();
        exec.execute(() -> {
            if (role != RaftRole.LEADER) {
                fut.completeExceptionally(new IllegalStateException("not leader, leaderId=" + leaderId));
                return;
            }
            long idx = logStore.size() + 1;
            LogEntry entry = new LogEntry(idx, currentTerm, cmd);
            logStore.add(entry);
            metrics.setLogSize(logStore.size());
            // TODO: persist entry to durable log via persistence when implemented
            // replicate to peers
            replicateToAll();
            outstanding.put(idx, fut);
        });
        return fut;
    }

    // -------------------------
    // RPC handlers (called inside exec)
    // -------------------------
    private void handleAppendEntries(TransportMessage tm, AppendEntriesRequest req) {
        metrics.incrementAppendEntriesReceived();
        // standard Raft checks
        if (req.term() < currentTerm) {
            sendAppendEntriesResponse(tm.from(), new AppendEntriesResponse(currentTerm, false, logStore.size()), tm.correlationId());
            return;
        }
        // if request term >= currentTerm, update and become follower
        if (req.term() > currentTerm) {
            currentTerm = req.term();
            votedFor = null;
            try { persistence.saveTerm(currentTerm); } catch (Exception e) { log.warn("saveTerm failed", e); }
            metrics.setCurrentTerm(currentTerm);
        }
        // accept leader
        role = RaftRole.FOLLOWER;
        leaderId = req.leaderId();
        metrics.setNodeState(role.ordinal());
        // reset election timer
        scheduleElectionTimeout();
        // check prev log
        if (req.prevLogIndex() > logStore.size()) {
            sendAppendEntriesResponse(tm.from(), new AppendEntriesResponse(currentTerm, false, logStore.size()), tm.correlationId());
            return;
        }
        if (req.prevLogIndex() > 0) {
            LogEntry prev = logStore.get((int) req.prevLogIndex() - 1);
            if (prev.term() != req.prevLogTerm()) {
                // conflict: delete suffix
                while (logStore.size() >= req.prevLogIndex()) logStore.removeLast();
                sendAppendEntriesResponse(tm.from(), new AppendEntriesResponse(currentTerm, false, logStore.size()), tm.correlationId());
                return;
            }
        }
        // append entries (replace conflicts)
        for (LogEntry e : req.entries()) {
            long idx = e.index();
            if (idx <= logStore.size()) {
                LogEntry exist = logStore.get((int) idx - 1);
                if (exist.term() != e.term()) {
                    while (logStore.size() >= idx) logStore.removeLast();
                    logStore.add(e);
                }
            } else {
                logStore.add(e);
            }
        }
        metrics.setLogSize(logStore.size());
        // update commit index and apply
        if (req.leaderCommit() > commitIndex) {
            commitIndex = Math.min(req.leaderCommit(), logStore.size());
            metrics.setCommitIndex(commitIndex);
            applyEntriesUpTo(commitIndex);
        }
        sendAppendEntriesResponse(tm.from(), new AppendEntriesResponse(currentTerm, true, logStore.size()), tm.correlationId());
    }

    private void handleAppendEntriesResponse(TransportMessage tm, AppendEntriesResponse resp) {
        // only leader cares
        if (resp.term() > currentTerm) {
            currentTerm = resp.term();
            try { persistence.saveTerm(currentTerm); } catch (Exception e) { log.warn("saveTerm failed", e); }
            metrics.setCurrentTerm(currentTerm);
            stepDownToFollower(null);
            return;
        }
        if (role != RaftRole.LEADER) return;
        String peer = tm.from();
        if (resp.success()) {
            matchIndex.put(peer, resp.matchIndex());
            nextIndex.put(peer, resp.matchIndex() + 1);
            metrics.incrementAppendEntriesSuccess();
            // try advance commit
            advanceCommitIndex();
        } else {
            metrics.incrementAppendEntriesFailed();
            long ni = nextIndex.getOrDefault(peer, (long) logStore.size());
            nextIndex.put(peer, Math.max(1L, ni - 1L));
            // retry replication soon
            scheduler.schedule(this::replicateToAll, 50, TimeUnit.MILLISECONDS);
        }
    }

    private void handleRequestVote(TransportMessage tm, VoteRequest req) {
        boolean grant = false;
        if (req.term() < currentTerm) {
            grant = false;
        } else {
            if ((votedFor == null || votedFor.equals(req.candidateId())) && isUpToDate(req.lastLogIndex(), req.lastLogTerm())) {
                grant = true;
                votedFor = req.candidateId();
                try { persistence.saveVotedFor(votedFor); } catch (Exception e) { log.warn("saveVotedFor failed", e); }
                if (req.term() > currentTerm) {
                    currentTerm = req.term();
                    try { persistence.saveTerm(currentTerm); } catch (Exception e) { log.warn("saveTerm failed", e); }
                    metrics.setCurrentTerm(currentTerm);
                }
            }
        }
        if (grant) metrics.incrementVoteGranted(); else metrics.incrementVoteDenied();
        VoteResponse resp = new VoteResponse(currentTerm, grant);
        byte[] payload = encodePayload(resp); // use codec
        transport.send(new TransportMessage(MessageType.REQUEST_VOTE_RESPONSE, nodeId, tm.from(), tm.correlationId(), payload));
    }

    private void handleRequestVoteResponse(TransportMessage tm, VoteResponse resp) {
        if (resp.term() > currentTerm) {
            currentTerm = resp.term();
            try { persistence.saveTerm(currentTerm); } catch (Exception e) { log.warn("saveTerm failed", e); }
            metrics.setCurrentTerm(currentTerm);
            stepDownToFollower(null);
            return;
        }
        if (role != RaftRole.CANDIDATE) return;
        if (resp.voteGranted()) {
            votesGranted++;
            metrics.incrementVoteGranted();
            if (votesGranted > peers.size() / 2) {
                becomeLeader();
            }
        } else {
            metrics.incrementVoteDenied();
        }
    }

    // -------------------------
    // Election & heartbeat & replication
    // -------------------------
    private void scheduleElectionTimeout() {
        int timeout = ThreadLocalRandom.current().nextInt(minElectionMs, maxElectionMs + 1);
        if (electionFuture != null) electionFuture.cancel(false);
        electionFuture = scheduler.schedule(() -> exec.execute(this::onElectionTimeout), timeout, TimeUnit.MILLISECONDS);
        log.debug("{} scheduled election timeout in {}ms", nodeId, timeout);
    }

    private void onElectionTimeout() {
        if (role == RaftRole.LEADER) {
            scheduleElectionTimeout();
            return;
        }
        // start election
        currentTerm += 1;
        try { persistence.saveTerm(currentTerm); } catch (Exception e) { log.warn("saveTerm failed", e); }
        votedFor = nodeId;
        try { persistence.saveVotedFor(votedFor); } catch (Exception e) { log.warn("saveVotedFor failed", e); }
        role = RaftRole.CANDIDATE;
        metrics.incrementElectionsStarted(currentTerm);
        votesGranted = 1; // self
        metrics.setNodeState(role.ordinal());
        // broadcast VoteRequest
        long lastIndex = logStore.size();
        long lastTerm = lastIndex > 0 ? logStore.get((int) lastIndex - 1).term() : 0L;
        VoteRequest req = new VoteRequest(currentTerm, nodeId, lastIndex, lastTerm);
        byte[] payload = encodePayload(req);
        long corr = nextCorrelation();
        for (String p : peers) {
            transport.send(new TransportMessage(MessageType.REQUEST_VOTE, nodeId, p, corr, payload));
        }
        scheduleElectionTimeout();
    }

    private void becomeLeader() {
        role = RaftRole.LEADER;
        leaderId = nodeId;
        for (String p : peers) {
            nextIndex.put(p, (long) logStore.size() + 1);
            matchIndex.put(p, 0L);
        }
        metrics.incrementElectionsWon(currentTerm);
        metrics.setNodeState(role.ordinal());
        log.info("{} became leader (term={})", nodeId, currentTerm);
        // schedule heartbeats
        if (heartbeatFuture != null) heartbeatFuture.cancel(false);
        heartbeatFuture = scheduler.scheduleAtFixedRate(() -> exec.execute(this::sendHeartbeats), 0, heartbeatMs, TimeUnit.MILLISECONDS);
    }

    private void stepDownToFollower(String leader) {
        if (role == RaftRole.LEADER) metrics.incrementStepDowns(currentTerm);
        role = RaftRole.FOLLOWER;
        leaderId = leader;
        votedFor = null;
        try { persistence.saveVotedFor(null); } catch (Exception e) { log.warn("saveVotedFor failed", e); }
        metrics.setNodeState(role.ordinal());
        if (heartbeatFuture != null) {
            heartbeatFuture.cancel(false);
            heartbeatFuture = null;
        }
        scheduleElectionTimeout();
        log.info("{} stepped down to follower (term={}) leader={}", nodeId, currentTerm, leader);
    }

    private void sendHeartbeats() {
        if (role != RaftRole.LEADER) return;
        for (String p : peers) {
            AppendEntriesRequest req = new AppendEntriesRequest(currentTerm, nodeId,
                    logStore.size(), !logStore.isEmpty() ? logStore.getLast().term() : 0L,
                    Collections.emptyList(), commitIndex);
            byte[] payload = encodePayload(req);
            transport.send(new TransportMessage(MessageType.APPEND_ENTRIES, nodeId, p, nextCorrelation(), payload));
            metrics.incrementHeartbeatsSent();
            metrics.incrementAppendEntriesSent();
        }
    }

    private void replicateToAll() {
        if (role != RaftRole.LEADER) return;
        long lastIdx = logStore.size();
        for (String p : peers) {
            long next = nextIndex.getOrDefault(p, lastIdx + 1);
            List<LogEntry> entries = entriesFrom(next);
            long prevIdx = next - 1;
            long prevTerm = prevIdx > 0 && prevIdx <= logStore.size() ? logStore.get((int) prevIdx - 1).term() : 0L;
            AppendEntriesRequest req = new AppendEntriesRequest(currentTerm, nodeId, prevIdx, prevTerm, entries, commitIndex);
            byte[] payload = encodePayload(req);
            transport.send(new TransportMessage(MessageType.APPEND_ENTRIES, nodeId, p, nextCorrelation(), payload));
            metrics.incrementAppendEntriesSent();
        }
    }

    private List<LogEntry> entriesFrom(long fromIndex) {
        long start = Math.max(fromIndex, 1L);
        int s = (int) start - 1;
        if (s >= logStore.size()) return Collections.emptyList();
        return new ArrayList<>(logStore.subList(s, logStore.size()));
    }

    // -------------------------
    // commit/apply
    // -------------------------
    private void advanceCommitIndex() {
        long N = commitIndex + 1;
        while (N <= logStore.size()) {
            int count = 1;
            for (String p : peers) {
                long m = matchIndex.getOrDefault(p, 0L);
                if (m >= N) count++;
            }
            if (count > peers.size() / 2) {
                LogEntry e = logStore.get((int) N - 1);
                if (e.term() == currentTerm) {
                    commitIndex = N;
                    metrics.setCommitIndex(commitIndex);
                    applyEntriesUpTo(commitIndex);
                    CompletableFuture<Long> f = outstanding.remove(N);
                    if (f != null) f.complete(N);
                    N++;
                    continue;
                }
            }
            break;
        }
    }

    private void applyEntriesUpTo(long target) {
        while (lastApplied < target) {
            lastApplied++;
            LogEntry e = logStore.get((int) lastApplied - 1);
            try {
                stateMachine.apply(e);
                metrics.incrementCommandsApplied();
                metrics.setLastApplied(lastApplied);
            } catch (Throwable t) {
                metrics.incrementCommandsFailed();
                log.error("{} apply failed at index={}", nodeId, e.index(), t);
            }
        }
    }

    // -------------------------
    // codec helpers (use RaftMessageCodec via EmbeddedChannel)
    // -------------------------
    private byte[] encodePayload(Object rpc) {
        EmbeddedChannel ch = new EmbeddedChannel(new RaftMessageCodec());
        try {
            // write outbound object -> codec will produce ByteBuf outbound
            if (!ch.writeOutbound(rpc)) {
                throw new RuntimeException("Codec refused to encode rpc " + rpc.getClass());
            }
            ByteBuf buf = ch.readOutbound();
            try {
                int len = buf.readableBytes();
                byte[] b = new byte[len];
                buf.readBytes(b);
                return b;
            } finally {
                buf.release();
            }
        } finally {
            ch.finishAndReleaseAll();
        }
    }

    private Object decodePayload(byte[] payload) {
        EmbeddedChannel ch = new EmbeddedChannel(new RaftMessageCodec());
        try {
            ByteBuf in = Unpooled.wrappedBuffer(payload);
            if (!ch.writeInbound(in)) {
                return null;
            }
            Object obj = ch.readInbound();
            return obj;
        } finally {
            ch.finishAndReleaseAll();
        }
    }

    // -------------------------
    // send helpers
    // -------------------------
    private void sendAppendEntriesResponse(String to, AppendEntriesResponse resp, long corr) {
        byte[] payload = encodePayload(resp);
        transport.send(new TransportMessage(MessageType.APPEND_ENTRIES_RESPONSE, nodeId, to, corr, payload));
    }

    private long nextCorrelation() {
        return corrIdGen.getAndIncrement();
    }

    private boolean isUpToDate(long lastIndex, long lastTerm) {
        long myLastIndex = logStore.size();
        long myLastTerm = myLastIndex > 0 ? logStore.get((int) myLastIndex - 1).term() : 0L;
        if (lastTerm != myLastTerm) return lastTerm > myLastTerm;
        return lastIndex >= myLastIndex;
    }
}
