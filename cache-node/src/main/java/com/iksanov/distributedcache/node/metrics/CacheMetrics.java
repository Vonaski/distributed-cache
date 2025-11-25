package com.iksanov.distributedcache.node.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Metrics collector for cache-node using Micrometer.
 * Exposes cache statistics to Prometheus.
 */
public class CacheMetrics {

    private final PrometheusMeterRegistry registry;
    private final Counter cacheHits;
    private final Counter cacheMisses;
    private final Counter cacheEvictions;
    private final Timer getLatency;
    private final Timer putLatency;
    private final AtomicLong cacheSize = new AtomicLong(0);

    public CacheMetrics() {
        this.registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);

        this.cacheHits = Counter.builder("cache.hits")
                .description("Number of cache hits")
                .register(registry);
        
        this.cacheMisses = Counter.builder("cache.misses")
                .description("Number of cache misses")
                .register(registry);
        
        this.cacheEvictions = Counter.builder("cache.evictions")
                .description("Number of cache evictions")
                .register(registry);

        this.getLatency = Timer.builder("cache.get.duration")
                .description("GET operation duration")
                .publishPercentileHistogram()
                .serviceLevelObjectives(
                    java.time.Duration.ofMillis(1),
                    java.time.Duration.ofMillis(5),
                    java.time.Duration.ofMillis(10),
                    java.time.Duration.ofMillis(50),
                    java.time.Duration.ofMillis(100),
                    java.time.Duration.ofMillis(500)
                )
                .register(registry);

        this.putLatency = Timer.builder("cache.put.duration")
                .description("PUT operation duration")
                .publishPercentileHistogram()
                .serviceLevelObjectives(
                    java.time.Duration.ofMillis(1),
                    java.time.Duration.ofMillis(5),
                    java.time.Duration.ofMillis(10),
                    java.time.Duration.ofMillis(50),
                    java.time.Duration.ofMillis(100),
                    java.time.Duration.ofMillis(500)
                )
                .register(registry);

        Gauge.builder("cache.size", cacheSize, AtomicLong::get)
                .description("Current cache size")
                .register(registry);

        Gauge.builder("cache.hit.rate", this, CacheMetrics::calculateHitRate)
                .description("Cache hit rate percentage")
                .register(registry);
    }

    public void recordHit() {
        cacheHits.increment();
    }

    public void recordMiss() {
        cacheMisses.increment();
    }

    public void recordEviction() {
        cacheEvictions.increment();
    }

    public void updateSize(int size) {
        cacheSize.set(size);
    }

    public Timer.Sample startGetTimer() {
        return Timer.start(registry);
    }

    public void stopGetTimer(Timer.Sample sample) {
        sample.stop(getLatency);
    }

    public Timer.Sample startPutTimer() {
        return Timer.start(registry);
    }

    public void stopPutTimer(Timer.Sample sample) {
        sample.stop(putLatency);
    }

    private double calculateHitRate() {
        double hits = cacheHits.count();
        double misses = cacheMisses.count();
        double total = hits + misses;
        return total == 0 ? 0.0 : (hits / total) * 100.0;
    }

    public String scrape() {
        return registry.scrape();
    }

    public MeterRegistry getRegistry() {
        return registry;
    }
}