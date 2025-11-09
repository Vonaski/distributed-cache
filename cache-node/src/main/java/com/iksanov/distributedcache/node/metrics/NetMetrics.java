package com.iksanov.distributedcache.node.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Timer;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Metrics collector for Netty-based networking layer.
 * Exposes connection and request statistics to Prometheus.
 */
public class NetMetrics {

    private static final Logger log = LoggerFactory.getLogger(NetMetrics.class);
    private final PrometheusMeterRegistry registry;

    private final Counter totalConnections;
    private final Counter closedConnections;
    private final Counter totalRequests;
    private final Counter totalResponses;
    private final Counter requestErrors;
    private final Counter serverStartups;
    private final Counter serverShutdowns;
    private final Timer requestDuration;
    private final AtomicLong activeConnections = new AtomicLong(0);

    public NetMetrics() {
        this.registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);

        this.totalConnections = Counter.builder("net.connections.total")
                .description("Total TCP connections accepted")
                .register(registry);

        this.closedConnections = Counter.builder("net.connections.closed")
                .description("Total TCP connections closed")
                .register(registry);

        this.totalRequests = Counter.builder("net.requests.total")
                .description("Total number of received cache requests")
                .register(registry);

        this.totalResponses = Counter.builder("net.responses.total")
                .description("Total number of sent cache responses")
                .register(registry);

        this.requestErrors = Counter.builder("net.requests.errors")
                .description("Total number of failed request processing attempts")
                .register(registry);

        this.serverStartups = Counter.builder("net.server.startups")
                .description("Total times the NetServer was started")
                .register(registry);

        this.serverShutdowns = Counter.builder("net.server.shutdowns")
                .description("Total times the NetServer was stopped")
                .register(registry);

        this.requestDuration = Timer.builder("net.request.duration")
                .description("Request processing duration in milliseconds")
                .register(registry);

        Gauge.builder("net.connections.active", activeConnections, AtomicLong::get)
                .description("Current number of active TCP connections")
                .register(registry);

        log.info("✅ NetMetrics initialized and Prometheus registry created");
    }

    public void incrementConnections() { totalConnections.increment(); activeConnections.incrementAndGet(); }
    public void incrementClosedConnections() { closedConnections.increment(); activeConnections.decrementAndGet(); }
    public void incrementRequests() { totalRequests.increment(); }
    public void incrementResponses() { totalResponses.increment(); }
    public void incrementErrors() { requestErrors.increment(); }
    public void recordRequestDuration(long millis) { requestDuration.record(millis, java.util.concurrent.TimeUnit.MILLISECONDS); }
    public void serverStarted() { serverStartups.increment(); }
    public void serverStopped() { serverShutdowns.increment(); }

    public long getActiveConnections() { return activeConnections.get(); }
    public String scrape() { return registry.scrape(); }
    public PrometheusMeterRegistry getRegistry() { return registry; }
}
