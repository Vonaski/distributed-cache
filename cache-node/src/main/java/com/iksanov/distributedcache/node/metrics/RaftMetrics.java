package com.iksanov.distributedcache.node.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Metrics collector for Raft consensus using Micrometer.
 * Exposes Raft statistics to Prometheus for monitoring cluster health,
 * elections, leader changes, and replication status.
 */
public class RaftMetrics {
    private static final Logger log = LoggerFactory.getLogger(RaftMetrics.class);
    private final PrometheusMeterRegistry registry;
    private final Counter electionsStarted;
    private final Counter electionsWon;
    private final Counter electionsFailed;
    private final Counter votesGranted;
    private final Counter votesDenied;
    private final Counter leadersElected;
    private final Counter stepDowns;
    private final AtomicLong knownLeaderChanges = new AtomicLong(0);
    private final Counter heartbeatsSent;
    private final Counter heartbeatsReceived;
    private final Counter appendEntriesSent;
    private final Counter appendEntriesReceived;
    private final Counter appendEntriesSuccess;
    private final Counter appendEntriesFailed;
    private final AtomicLong currentTerm = new AtomicLong(0);
    private final AtomicLong commitIndex = new AtomicLong(0);
    private final AtomicLong lastApplied = new AtomicLong(0);
    private final AtomicLong logSize = new AtomicLong(0);
    private final AtomicLong nodeState = new AtomicLong(0);
    private final Counter commandsReplicated;
    private final Counter commandsApplied;
    private final Counter commandsFailed;

    public RaftMetrics() {
        this.registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        this.electionsStarted = Counter.builder("raft_elections_started_total")
                .description("Number of elections started by this node")
                .register(registry);
        this.electionsWon = Counter.builder("raft_elections_won_total")
                .description("Number of elections won by this node")
                .register(registry);
        this.electionsFailed = Counter.builder("raft_elections_failed_total")
                .description("Number of elections failed by this node")
                .register(registry);
        this.votesGranted = Counter.builder("raft_votes_granted_total")
                .description("Votes granted to this node during elections")
                .register(registry);
        this.votesDenied = Counter.builder("raft_votes_denied_total")
                .description("Votes denied to this node during elections")
                .register(registry);
        this.leadersElected = Counter.builder("raft_leaders_elected_total")
                .description("Number of times this node became leader")
                .register(registry);
        this.stepDowns = Counter.builder("raft_stepdowns_total")
                .description("Number of times this node stepped down from leadership")
                .register(registry);
        Gauge.builder("raft_known_leader_changes_total", knownLeaderChanges, AtomicLong::get)
                .description("Number of times the cluster's leader changed")
                .register(registry);
        this.heartbeatsSent = Counter.builder("raft_heartbeats_sent_total")
                .description("Total heartbeats sent by leader")
                .register(registry);
        this.heartbeatsReceived = Counter.builder("raft_heartbeats_received_total")
                .description("Total heartbeats received from leader")
                .register(registry);
        this.appendEntriesSent = Counter.builder("raft_append_entries_sent_total")
                .description("Total AppendEntries requests sent")
                .register(registry);
        this.appendEntriesReceived = Counter.builder("raft_append_entries_received_total")
                .description("Total AppendEntries requests received")
                .register(registry);
        this.appendEntriesSuccess = Counter.builder("raft_append_entries_success_total")
                .description("Total successful AppendEntries responses")
                .register(registry);
        this.appendEntriesFailed = Counter.builder("raft_append_entries_failed_total")
                .description("Total failed AppendEntries responses")
                .register(registry);
        Gauge.builder("raft_current_term", currentTerm, AtomicLong::get)
                .description("Current Raft term known by this node")
                .register(registry);
        Gauge.builder("raft_commit_index", commitIndex, AtomicLong::get)
                .description("Index of highest log entry known to be committed")
                .register(registry);
        Gauge.builder("raft_last_applied", lastApplied, AtomicLong::get)
                .description("Index of highest log entry applied to state machine")
                .register(registry);
        Gauge.builder("raft_log_size", logSize, AtomicLong::get)
                .description("Total number of entries in the Raft log")
                .register(registry);
        Gauge.builder("raft_node_state", nodeState, AtomicLong::get)
                .description("Current node state (0=FOLLOWER, 1=CANDIDATE, 2=LEADER)")
                .register(registry);
        this.commandsReplicated = Counter.builder("raft_commands_replicated_total")
                .description("Total commands successfully replicated to cluster")
                .register(registry);
        this.commandsApplied = Counter.builder("raft_commands_applied_total")
                .description("Total commands applied to state machine")
                .register(registry);
        this.commandsFailed = Counter.builder("raft_commands_failed_total")
                .description("Total commands failed to replicate")
                .register(registry);
        log.info("✅ RaftMetrics initialized and Prometheus registry created");
    }

    public void incrementElectionsStarted(long term) {
        electionsStarted.increment();
        currentTerm.set(term);
        log.debug("Election started (term={})", term);
    }

    public void incrementElectionsWon(long term) {
        electionsWon.increment();
        leadersElected.increment();
        currentTerm.set(term);
        log.debug("Node won election and became leader (term={})", term);
    }

    public void incrementElectionsFailed() {
        electionsFailed.increment();
        log.debug("Election failed");
    }

    public void incrementVoteGranted() {
        votesGranted.increment();
    }

    public void incrementVoteDenied() {
        votesDenied.increment();
    }

    public void incrementLeadersElected(long term) {
        leadersElected.increment();
        currentTerm.set(term);
        log.debug("Node became leader (term={})", term);
    }

    public void incrementStepDowns(long term) {
        stepDowns.increment();
        currentTerm.set(term);
        log.debug("Node stepped down (term={})", term);
    }

    public void leaderChanged() {
        knownLeaderChanges.incrementAndGet();
        log.debug("Leader changed in cluster");
    }

    public void incrementHeartbeatsSent() {
        heartbeatsSent.increment();
    }

    public void incrementHeartbeatsReceived() {
        heartbeatsReceived.increment();
    }

    public void incrementAppendEntriesSent() {
        appendEntriesSent.increment();
    }

    public void incrementAppendEntriesReceived() {
        appendEntriesReceived.increment();
    }

    public void incrementAppendEntriesSuccess() {
        appendEntriesSuccess.increment();
    }

    public void incrementAppendEntriesFailed() {
        appendEntriesFailed.increment();
    }

    public void setCurrentTerm(long term) {
        currentTerm.set(term);
    }

    public void setCommitIndex(long index) {
        commitIndex.set(index);
    }

    public void setLastApplied(long index) {
        lastApplied.set(index);
    }

    public void setLogSize(long size) {
        logSize.set(size);
    }

    public void setNodeState(int state) {
        nodeState.set(state);
    }

    public void incrementCommandsReplicated() {
        commandsReplicated.increment();
    }

    public void incrementCommandsApplied() {
        commandsApplied.increment();
    }

    public void incrementCommandsFailed() {
        commandsFailed.increment();
    }

    public String scrape() {
        return registry.scrape();
    }

    public PrometheusMeterRegistry getRegistry() {
        return registry;
    }
}