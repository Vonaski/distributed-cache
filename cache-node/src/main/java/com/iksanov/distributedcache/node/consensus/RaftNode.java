package com.iksanov.distributedcache.node.consensus;

import com.iksanov.distributedcache.node.config.RaftConfig;
import com.iksanov.distributedcache.node.consensus.model.*;
import com.iksanov.distributedcache.node.consensus.persistence.RaftPersistence;
import com.iksanov.distributedcache.node.consensus.transport.NettyRaftTransport;
import com.iksanov.distributedcache.node.metrics.RaftMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;


/**
 * Represents a single node participating in the Raft consensus algorithm.
 * <p>
 * Handles leader election and heartbeat exchange.
 */
public class RaftNode {
    private static final Logger log = LoggerFactory.getLogger(RaftNode.class);

    public enum NodeState {FOLLOWER, CANDIDATE, LEADER}

    private final String nodeId;
    private final List<String> peers;
    private final ReentrantLock stateLock = new ReentrantLock();
    private volatile NodeState state = NodeState.FOLLOWER;
    private volatile String currentLeader;
    private final AtomicLong currentTerm = new AtomicLong(0);
    private volatile String votedFor;
    private volatile long lastHeartbeat = 0L;
    private final RaftConfig config;
    private final RaftPersistence persistence;
    private final RaftMetrics metrics;
    private final ScheduledExecutorService scheduler;
    private final ExecutorService transportExecutor;
    private NettyRaftTransport transport;
    private final AtomicInteger runningFlag = new AtomicInteger(1);
    private ScheduledFuture<?> electionTimerFuture;
    private ScheduledFuture<?> heartbeatTimerFuture;
    private final CopyOnWriteArrayList<LeaderChangeListener> listeners = new CopyOnWriteArrayList<>();

    public RaftNode(String nodeId,
                    List<String> allNodes,
                    RaftConfig config,
                    RaftPersistence persistence,
                    RaftMetrics metrics,
                    int raftPort) {
        this.nodeId = Objects.requireNonNull(nodeId);
        this.peers = allNodes.stream().filter(n -> !n.equals(nodeId)).toList();
        this.config = (config != null) ? config : RaftConfig.defaults();
        this.persistence = (persistence != null) ? persistence : RaftPersistence.noop();
        this.metrics = (metrics != null) ? metrics : new RaftMetrics();
        this.scheduler = Executors.newScheduledThreadPool(2, r -> new Thread(r, "raft-scheduler-" + nodeId));
        this.transportExecutor = Executors.newCachedThreadPool(r -> new Thread(r, "raft-transport-" + nodeId));
        restoreStateFromPersistence();
        initTransport(raftPort);
        startElectionTimer();
        log.info("✅ RaftNode {} initialized with peers {} and transport port {}", nodeId, peers, raftPort);
    }

    private void initTransport(int raftPort) {
        try {
            transport = new NettyRaftTransport(raftPort, this, metrics);
            transport.startServer();
            log.info("🚀 [Raft] Transport started on port {}", raftPort);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Failed to start Raft transport on port " + raftPort, e);
        } catch (Exception e) {
            throw new RuntimeException("Error initializing Raft transport on port " + raftPort, e);
        }
    }

    private void restoreStateFromPersistence() {
        try {
            long term = persistence.loadTerm();
            if (term > 0) currentTerm.set(term);
            String vf = persistence.loadVotedFor();
            if (vf != null && !vf.isBlank()) votedFor = vf;
            log.info("🔁 [Raft] Node {} restored term={} votedFor={}", nodeId, currentTerm.get(), votedFor);
        } catch (Exception e) {
            log.warn("⚠️ [Raft] Failed to restore Raft state for {}: {}", nodeId, e.getMessage());
        }
    }

    private void startElectionTimer() {
        stateLock.lock();
        try {
            if (electionTimerFuture != null && !electionTimerFuture.isDone()) electionTimerFuture.cancel(false);
            long timeout = ThreadLocalRandom.current().nextLong(
                    config.electionTimeoutMinMs(), config.electionTimeoutMaxMs() + 1);
            electionTimerFuture = scheduler.schedule(this::startElection, timeout, TimeUnit.MILLISECONDS);
        } finally {
            stateLock.unlock();
        }
    }

    private void startElection() {
        if (runningFlag.get() == 0) return;

        stateLock.lock();
        long newTerm;
        try {
            if (state == NodeState.LEADER) {
                startElectionTimer();
                return;
            }
            newTerm = currentTerm.incrementAndGet();
            persistence.saveTerm(newTerm);
            state = NodeState.CANDIDATE;
            votedFor = nodeId;
            persistence.saveVotedFor(nodeId);
            currentLeader = null;
            log.info("🗳️ [Raft] Node {} started election for term {}", nodeId, newTerm);
            metrics.incrementElectionsStarted(newTerm);
        } catch (Exception e) {
            log.error("💥 [Raft] Error starting election: {}", e.getMessage());
            return;
        } finally {
            stateLock.unlock();
        }

        AtomicInteger votes = new AtomicInteger(1);
        int votesNeeded = (peers.size() + 1) / 2 + 1;

        for (String peer : peers) {
            transport.requestVote(peer, new VoteRequest(newTerm, nodeId),
                            Duration.ofMillis(config.voteWaitTimeoutMs()))
                    .whenCompleteAsync((resp, ex) -> {
                        if (ex != null) {
                            log.debug("⚠️ [Raft] Vote request to {} failed: {}", peer, ex.getMessage());
                            return;
                        }
                        if (resp.term > newTerm) {
                            stepDown(resp.term);
                        } else if (resp.voteGranted) {
                            metrics.incrementVoteGranted();
                            int v = votes.incrementAndGet();
                            log.debug("📨 [Raft] Node {} got vote from {} ({}/{})", nodeId, peer, v, votesNeeded);
                            if (v >= votesNeeded) {
                                becomeLeader();
                            }
                        } else {
                            metrics.incrementVoteDenied();
                        }
                    }, transportExecutor);
        }
    }

    private void becomeLeader() {
        stateLock.lock();
        try {
            if (state != NodeState.CANDIDATE) return;
            state = NodeState.LEADER;
            currentLeader = nodeId;
            if (electionTimerFuture != null) electionTimerFuture.cancel(false);
            metrics.incrementLeadersElected(currentTerm.get());
            log.info("👑 [Raft] Node {} became LEADER for term {}", nodeId, currentTerm.get());
            startHeartbeatTimer();
            listeners.forEach(l -> safeCall(() -> l.onBecomeLeader()));
        } finally {
            stateLock.unlock();
        }
    }

    private void stepDown(long newTerm) {
        stateLock.lock();
        try {
            if (newTerm <= currentTerm.get()) return;
            currentTerm.set(newTerm);
            persistence.saveTerm(newTerm);
            NodeState prev = state;
            state = NodeState.FOLLOWER;
            votedFor = null;
            persistence.saveVotedFor(nodeId);
            log.warn("⚠️ [Raft] Node {} stepping down to FOLLOWER (new term {})", nodeId, newTerm);
            if (prev == NodeState.LEADER) {
                metrics.incrementStepDowns(newTerm);
                listeners.forEach(l -> safeCall(() -> l.onLoseLeadership()));
            }
            currentLeader = null;
            if (heartbeatTimerFuture != null) heartbeatTimerFuture.cancel(false);
            startElectionTimer();
        } catch (Exception e) {
            log.error("💥 [Raft] Error during stepDown: {}", e.getMessage());
        } finally {
            stateLock.unlock();
        }
    }

    private void startHeartbeatTimer() {
        if (heartbeatTimerFuture != null && !heartbeatTimerFuture.isDone()) heartbeatTimerFuture.cancel(false);
        heartbeatTimerFuture = scheduler.scheduleAtFixedRate(() -> {
            if (state != NodeState.LEADER || runningFlag.get() == 0) return;
            log.trace("💓 [Raft] Node {} sending heartbeats to {} peers", nodeId, peers.size());
            metrics.incrementHeartbeatsSent();
            for (String peer : peers) {
                transport.sendHeartbeat(peer,
                                new HeartbeatRequest(currentTerm.get(), nodeId),
                                Duration.ofMillis(config.heartbeatIntervalMs()))
                        .whenCompleteAsync((resp, ex) -> {
                            if (ex != null) {
                                log.trace("⚠️ [Raft] Heartbeat to {} failed: {}", peer, ex.getMessage());
                                return;
                            }
                            if (resp.term > currentTerm.get()) stepDown(resp.term);
                            metrics.incrementHeartbeatsReceived();
                        }, transportExecutor);
            }
        }, 0, config.heartbeatIntervalMs(), TimeUnit.MILLISECONDS);
    }

    public VoteResponse handleVoteRequest(VoteRequest req) {
        stateLock.lock();
        try {
            VoteResponse resp = new VoteResponse();
            resp.term = currentTerm.get();
            if (req.term() < currentTerm.get()) return resp;
            if (req.term() > currentTerm.get()) stepDown(req.term());
            if (votedFor == null || votedFor.equals(req.candidateId())) {
                votedFor = req.candidateId();
                persistence.saveVotedFor(votedFor);
                startElectionTimer();
                resp.voteGranted = true;
                log.debug("🗳️ [Raft] Node {} grants vote to {} for term {}", nodeId, req.candidateId(), req.term());
                metrics.incrementVoteGranted();
            } else {
                log.debug("🚫 [Raft] Node {} denies vote to {} (already voted for {})", nodeId, req.candidateId(), votedFor);
                resp.voteGranted = false;
                metrics.incrementVoteDenied();
            }
            return resp;
        } catch (Exception e) {
            log.error("💥 [Raft] Error handling VoteRequest: {}", e.getMessage());
            throw new RuntimeException(e);
        } finally {
            stateLock.unlock();
        }
    }

    public HeartbeatResponse handleHeartbeat(HeartbeatRequest req) {
        stateLock.lock();
        try {
            HeartbeatResponse resp = new HeartbeatResponse();
            resp.term = currentTerm.get();
            if (req.term() < currentTerm.get()) return resp;
            if (req.term() > currentTerm.get()) stepDown(req.term());
            currentLeader = req.leaderId();
            lastHeartbeat = System.currentTimeMillis();
            resp.success = true;
            log.trace("💓 [Raft] Node {} received heartbeat from leader {} (term={})", nodeId, req.leaderId(), req.term());
            metrics.incrementHeartbeatsReceived();
            startElectionTimer();
            return resp;
        } catch (Exception e) {
            log.error("💥 [Raft] Error handling Heartbeat: {}", e.getMessage());
            throw new RuntimeException(e);
        } finally {
            stateLock.unlock();
        }
    }

    private void safeCall(Runnable r) {
        try {
            r.run();
        } catch (Exception ignored) {
        }
    }

    public void shutdown() {
        if (!runningFlag.compareAndSet(1, 0)) return;
        log.info("🧩 [Raft] Node {} shutting down", nodeId);
        if (electionTimerFuture != null) electionTimerFuture.cancel(true);
        if (heartbeatTimerFuture != null) heartbeatTimerFuture.cancel(true);
        scheduler.shutdownNow();
        transportExecutor.shutdownNow();
        if (transport != null) transport.shutdown();
        log.info("✅ [Raft] Node {} shutdown complete", nodeId);
    }
}
