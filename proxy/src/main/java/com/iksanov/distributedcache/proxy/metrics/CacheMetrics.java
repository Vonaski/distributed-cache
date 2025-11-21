package com.iksanov.distributedcache.proxy.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicInteger;

@Component
public class CacheMetrics {

    private final Counter requestsTotal;
    private final Counter requestsSuccess;
    private final Counter requestsFailed;
    private final Counter retries;
    private final Timer requestDuration;
    private final Counter failoversTotal;
    private final Counter healthChecksFailed;
    private final AtomicInteger activeFailovers = new AtomicInteger(0);

    public CacheMetrics(MeterRegistry registry) {
        this.requestsTotal = Counter.builder("cache.requests.total")
                .description("Total number of cache requests")
                .tag("type", "all")
                .register(registry);

        this.requestsSuccess = Counter.builder("cache.requests.success")
                .description("Number of successful cache requests")
                .register(registry);

        this.requestsFailed = Counter.builder("cache.requests.failed")
                .description("Number of failed cache requests")
                .register(registry);

        this.retries = Counter.builder("cache.retries.total")
                .description("Total number of request retries")
                .register(registry);

        this.requestDuration = Timer.builder("cache.request.duration")
                .description("Duration of cache requests")
                .register(registry);

        // Failover metrics
        this.failoversTotal = Counter.builder("cache.failovers.total")
                .description("Total number of failover events")
                .register(registry);

        this.healthChecksFailed = Counter.builder("cache.healthchecks.failed")
                .description("Number of failed health checks")
                .register(registry);

        Gauge.builder("cache.failovers.active", activeFailovers, AtomicInteger::get)
                .description("Number of currently failed masters")
                .register(registry);
    }

    public void recordRequest() {
        requestsTotal.increment();
    }

    public void recordSuccess() {
        requestsSuccess.increment();
    }

    public void recordFailure() {
        requestsFailed.increment();
    }

    public void recordRetry() {
        retries.increment();
    }

    public Timer.Sample startTimer() {
        return Timer.start();
    }

    public void stopTimer(Timer.Sample sample) {
        sample.stop(requestDuration);
    }

    public void recordFailover() {
        failoversTotal.increment();
        activeFailovers.incrementAndGet();
    }

    public void recordHealthCheckFailure() {
        healthChecksFailed.increment();
    }

    public void recordMasterRestored() {
        activeFailovers.updateAndGet(current -> Math.max(0, current - 1));
    }
}