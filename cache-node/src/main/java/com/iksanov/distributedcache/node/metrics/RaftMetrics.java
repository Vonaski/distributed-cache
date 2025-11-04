package com.iksanov.distributedcache.node.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Metrics collector for Raft-consensus using Micrometer.
 * Exposes cache statistics to Prometheus.
 */
public class RaftMetrics {
    private static final Logger log = LoggerFactory.getLogger(RaftMetrics.class);
    private final PrometheusMeterRegistry registry;
    private final Counter electionsStarted;
    private final Counter leadersElected;
    private final Counter stepDowns;
    private final Counter heartbeatsSent;
    private final Counter heartbeatsReceived;
    private final Counter votesGranted;
    private final Counter votesDenied;
    private final AtomicLong currentTerm = new AtomicLong(0);
    private final AtomicLong knownLeaderChanges = new AtomicLong(0);

    public RaftMetrics() {
        this.registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);

        this.electionsStarted = Counter.builder("raft_elections_started_total")
                .description("Number of elections started by this node")
                .register(registry);

        this.leadersElected = Counter.builder("raft_leaders_elected_total")
                .description("Number of times this node became leader")
                .register(registry);

        this.stepDowns = Counter.builder("raft_stepdowns_total")
                .description("Number of times this node stepped down from leadership")
                .register(registry);

        this.heartbeatsSent = Counter.builder("raft_heartbeats_sent_total")
                .description("Total heartbeats sent by leader")
                .register(registry);

        this.heartbeatsReceived = Counter.builder("raft_heartbeats_received_total")
                .description("Total heartbeats received from leader")
                .register(registry);

        this.votesGranted = Counter.builder("raft_votes_granted_total")
                .description("Votes granted to this node during elections")
                .register(registry);

        this.votesDenied = Counter.builder("raft_votes_denied_total")
                .description("Votes denied to this node during elections")
                .register(registry);

        Gauge.builder("raft_current_term", currentTerm, AtomicLong::get)
                .description("Current Raft term known by this node")
                .register(registry);

        Gauge.builder("raft_known_leader_changes_total", knownLeaderChanges, AtomicLong::get)
                .description("Number of times the cluster's leader changed")
                .register(registry);

        log.info("✅ RaftMetrics initialized and Prometheus registry created");
    }

    public void incrementElectionsStarted(long term) {
        electionsStarted.increment();
        currentTerm.set(term);
        log.debug("Election started (term={})", term);
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

    public void incrementHeartbeatsSent() {
        heartbeatsSent.increment();
    }

    public void incrementHeartbeatsReceived() {
        heartbeatsReceived.increment();
    }

    public void incrementVoteGranted() {
        votesGranted.increment();
    }

    public void incrementVoteDenied() {
        votesDenied.increment();
    }

    public void leaderChanged() {
        knownLeaderChanges.incrementAndGet();
    }

    public String scrape() {
        return registry.scrape();
    }

    public PrometheusMeterRegistry getRegistry() {
        return registry;
    }
}
