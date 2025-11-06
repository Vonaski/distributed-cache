package com.iksanov.distributedcache.node.consensus;

import com.iksanov.distributedcache.node.config.RaftConfig;
import com.iksanov.distributedcache.node.consensus.model.*;
import com.iksanov.distributedcache.node.consensus.persistence.RaftPersistence;
import com.iksanov.distributedcache.node.consensus.transport.NettyRaftTransport;
import com.iksanov.distributedcache.node.metrics.RaftMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

public class RaftNode {
    private static final Logger log = LoggerFactory.getLogger(RaftNode.class);
    public enum NodeState {FOLLOWER, CANDIDATE, LEADER}
    private final String nodeId;
    private final List<String> peers;
    private final Map<String, String> peerAddresses;
    private final ReentrantLock stateLock = new ReentrantLock();
    private volatile NodeState state = NodeState.FOLLOWER;
    private volatile String currentLeader;
    private final AtomicLong currentTerm = new AtomicLong(0);
    private volatile String votedFor;
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
        this(nodeId, allNodes, buildDefaultAddresses(allNodes), config, persistence, metrics, raftPort);
    }

    public RaftNode(String nodeId,
                    List<String> allNodes,
                    Map<String, String> peerAddresses,
                    RaftConfig config,
                    RaftPersistence persistence,
                    RaftMetrics metrics,
                    int raftPort) {
        this.nodeId = Objects.requireNonNull(nodeId);
        this.peers = allNodes.stream().filter(n -> !n.equals(nodeId)).toList();
        this.peerAddresses = peerAddresses != null ? peerAddresses : buildDefaultAddresses(allNodes);
        this.config = (config != null) ? config : RaftConfig.defaults();
        this.persistence = (persistence != null) ? persistence : RaftPersistence.noop();
        this.metrics = (metrics != null) ? metrics : new RaftMetrics();
        this.scheduler = Executors.newScheduledThreadPool(2, r -> new Thread(r, "raft-scheduler-" + nodeId));
        this.transportExecutor = Executors.newCachedThreadPool(r -> new Thread(r, "raft-transport-" + nodeId));
        restoreStateFromPersistence();
        initTransport(raftPort);
        startElectionTimer();
        log.info("[Raft] Node {} initialized with peers {} and transport port {}", nodeId, peers, raftPort);
    }

    private static Map<String, String> buildDefaultAddresses(List<String> allNodes) {
        Map<String, String> addresses = new HashMap<>();
        for (String nodeId : allNodes) {
            addresses.put(nodeId, nodeId);
        }
        return addresses;
    }

    private void initTransport(int raftPort) {
        try {
            transport = new NettyRaftTransport(raftPort, this, metrics);
            transport.startServer();
            log.info("[Raft] Transport started on port {}", raftPort);
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
            log.info("[Raft] Node {} restored term={} votedFor={}", nodeId, currentTerm.get(), votedFor);
        } catch (Exception e) {
            log.warn("[Raft] Failed to restore Raft state for {}: {}", nodeId, e.getMessage());
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
        long electionTerm;
        try {
            if (state == NodeState.LEADER) {
                startElectionTimer();
                return;
            }
            long currentTermValue = currentTerm.get();
            if (currentTermValue == Long.MAX_VALUE) {
                log.error("[Raft] Node {} cannot increment term - already at MAX_VALUE", nodeId);
                state = NodeState.FOLLOWER;
                startElectionTimer();
                return;
            }
            electionTerm = currentTerm.incrementAndGet();
            persistence.saveTerm(electionTerm);
            state = NodeState.CANDIDATE;
            votedFor = nodeId;
            persistence.saveVotedFor(nodeId);
            currentLeader = null;
            log.info("[Raft] Node {} started election for term {}", nodeId, electionTerm);
            metrics.incrementElectionsStarted(electionTerm);
        } catch (Exception e) {
            log.error("[Raft] Error starting election: {}", e.getMessage());
            return;
        } finally {
            stateLock.unlock();
        }

        AtomicInteger votes = new AtomicInteger(1);
        int votesNeeded = (peers.size() + 1) / 2 + 1;

        if (peers.isEmpty()) {
            becomeLeader(electionTerm);
            return;
        }

        for (String peer : peers) {
            String peerAddress = peerAddresses.getOrDefault(peer, peer);
            transport.requestVote(peerAddress, new VoteRequest(electionTerm, nodeId),
                            Duration.ofMillis(config.voteWaitTimeoutMs()))
                    .whenCompleteAsync((resp, ex) -> {
                        if (ex != null) {
                            log.debug("[Raft] Vote request to {} failed: {}", peer, ex.getMessage());
                            return;
                        }

                        if (currentTerm.get() != electionTerm) {
                            log.debug("[Raft] Ignoring stale vote response (term changed)");
                            return;
                        }

                        if (resp.term() > electionTerm) {
                            try {
                                stepDown(resp.term());
                            } catch (Exception e) {
                                log.error("[Raft] Error in stepDown: {}", e.getMessage());
                            }
                        } else if (resp.term() == electionTerm && resp.voteGranted()) {
                            int v = votes.incrementAndGet();
                            log.debug("[Raft] Node {} got vote from {} ({}/{})", nodeId, peer, v, votesNeeded);
                            if (v >= votesNeeded) {
                                becomeLeader(electionTerm);
                            }
                        }
                    }, transportExecutor);
        }
    }

    private void becomeLeader(long term) {
        stateLock.lock();
        try {
            if (state == NodeState.LEADER) return;
            if (currentTerm.get() != term) return;
            state = NodeState.LEADER;
            currentLeader = nodeId;
            log.info("[Raft] Node {} became LEADER for term {}", nodeId, term);
            metrics.incrementLeadersElected(currentTerm.get());
            listeners.forEach(l -> safeCall(() -> l.onBecomeLeader()));
            listeners.forEach(l -> safeCall(() -> l.onLeaderChange(nodeId)));
            startHeartbeatTimer();
        } finally {
            stateLock.unlock();
        }
    }

    private void stepDown(long newTerm) throws IOException {
        stateLock.lock();
        try {
            if (newTerm <= currentTerm.get()) return;
            currentTerm.set(newTerm);
            NodeState prev = state;
            state = NodeState.FOLLOWER;
            votedFor = null;
            currentLeader = null;
            persistence.saveTerm(newTerm);
            persistence.saveVotedFor(null);
            log.warn("[Raft] Node {} stepping down to FOLLOWER (new term {})", nodeId, newTerm);
            if (prev == NodeState.LEADER) {
                metrics.incrementStepDowns(newTerm);
                listeners.forEach(l -> safeCall(() -> l.onLoseLeadership()));
            }
            if (heartbeatTimerFuture != null) heartbeatTimerFuture.cancel(false);
            startElectionTimer();
        } finally {
            stateLock.unlock();
        }
    }

    private void startHeartbeatTimer() {
        if (heartbeatTimerFuture != null && !heartbeatTimerFuture.isDone()) heartbeatTimerFuture.cancel(false);
        heartbeatTimerFuture = scheduler.scheduleAtFixedRate(() -> {
            if (state != NodeState.LEADER || runningFlag.get() == 0) return;
            log.trace("[Raft] Node {} sending heartbeats to {} peers", nodeId, peers.size());
            metrics.incrementHeartbeatsSent();
            for (String peer : peers) {
                String peerAddress = peerAddresses.getOrDefault(peer, peer);
                transport.sendHeartbeat(peerAddress,
                                new HeartbeatRequest(currentTerm.get(), nodeId),
                                Duration.ofMillis(config.heartbeatIntervalMs()))
                        .whenCompleteAsync((resp, ex) -> {
                            if (ex != null) {
                                log.trace("[Raft] Heartbeat to {} failed: {}", peer, ex.getMessage());
                                return;
                            }
                            if (resp.term() > currentTerm.get()) {
                                try {
                                    stepDown(resp.term());
                                } catch (Exception e) {
                                    log.error("[Raft] Error in stepDown: {}", e.getMessage());
                                }
                            }
                            metrics.incrementHeartbeatsReceived();
                        }, transportExecutor);
            }
        }, 0, config.heartbeatIntervalMs(), TimeUnit.MILLISECONDS);
    }

    public VoteResponse handleVoteRequest(VoteRequest req) {
        stateLock.lock();
        try {
            if (!peers.contains(req.candidateId()) && !req.candidateId().equals(nodeId)) {
                log.debug("[Raft] Node {} denies vote to UNKNOWN candidate {}", nodeId, req.candidateId());
                metrics.incrementVoteDenied();
                return new VoteResponse(currentTerm.get(), false);
            }
            if (req.term() < currentTerm.get()) {
                return new VoteResponse(currentTerm.get(), false);
            }
            if (req.term() > currentTerm.get()) stepDown(req.term());

            if (votedFor == null || votedFor.equals(req.candidateId())) {
                votedFor = req.candidateId();
                persistence.saveVotedFor(votedFor);
                startElectionTimer();
                log.debug("[Raft] Node {} grants vote to {} for term {}", nodeId, req.candidateId(), req.term());
                metrics.incrementVoteGranted();
                return new VoteResponse(currentTerm.get(), true);
            } else {
                log.debug("[Raft] Node {} denies vote to {} (already voted for {})", nodeId, req.candidateId(), votedFor);
                metrics.incrementVoteDenied();
                return new VoteResponse(currentTerm.get(), false);
            }
        } catch (Exception e) {
            log.error("[Raft] Error handling VoteRequest: {}", e.getMessage(), e);
            return new VoteResponse(currentTerm.get(), false);
        } finally {
            stateLock.unlock();
        }
    }

    public HeartbeatResponse handleHeartbeat(HeartbeatRequest req) {
        stateLock.lock();
        try {
            if (!peers.contains(req.leaderId()) && !req.leaderId().equals(nodeId)) {
                log.debug("[Raft] Node {} ignores heartbeat from UNKNOWN leader {}", nodeId, req.leaderId());
                return new HeartbeatResponse(currentTerm.get(), false);
            }
            if (req.term() < currentTerm.get()) {
                return new HeartbeatResponse(currentTerm.get(), false);
            }
            if (req.term() > currentTerm.get()) {
                stepDown(req.term());
            }
            currentLeader = req.leaderId();
            log.trace("[Raft] Node {} received heartbeat from leader {} (term={})", nodeId, req.leaderId(), req.term());
            metrics.incrementHeartbeatsReceived();
            startElectionTimer();
            return new HeartbeatResponse(currentTerm.get(), true);
        } catch (Exception e) {
            log.error("[Raft] Error handling Heartbeat: {}", e.getMessage(), e);
            return new HeartbeatResponse(currentTerm.get(), false);
        } finally {
            stateLock.unlock();
        }
    }

    private void safeCall(Runnable r) {
        try {
            r.run();
        } catch (Exception e) {
            log.warn("[Raft] Error in listener callback: {}", e.getMessage());
        }
    }

    public void addLeaderChangeListener(LeaderChangeListener listener) {
        listeners.add(listener);
    }

    public NodeState getState() {
        return state;
    }

    public long getCurrentTerm() {
        return currentTerm.get();
    }

    public String getCurrentLeader() {
        return currentLeader;
    }

    public String getNodeId() {
        return nodeId;
    }

    public void shutdown() {
        if (!runningFlag.compareAndSet(1, 0)) return;
        log.info("[Raft] Node {} shutting down", nodeId);
        try {
            if (electionTimerFuture != null) electionTimerFuture.cancel(true);
            if (heartbeatTimerFuture != null) heartbeatTimerFuture.cancel(true);
            scheduler.shutdownNow();
            scheduler.awaitTermination(1, TimeUnit.SECONDS);
            transportExecutor.shutdownNow();
            transportExecutor.awaitTermination(1, TimeUnit.SECONDS);
            if (transport != null) {
                try {
                    transport.shutdown();
                } catch (Exception e) {
                    log.warn("[Raft] Transport shutdown failed: {}", e.getMessage());
                }
            }
            metrics.incrementStepDowns(currentTerm.get());
            log.info("[Raft] Node {} shutdown complete", nodeId);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("[Raft] Interrupted during shutdown");
        }
    }
}