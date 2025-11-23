package com.iksanov.distributedcache.proxy.service;

import com.iksanov.distributedcache.common.cluster.NodeInfo;
import com.iksanov.distributedcache.common.dto.CacheRequest;
import com.iksanov.distributedcache.common.dto.CacheResponse;
import com.iksanov.distributedcache.common.exception.CacheConnectionException;
import com.iksanov.distributedcache.common.exception.InvalidCacheRequestException;
import com.iksanov.distributedcache.proxy.client.CacheClient;
import com.iksanov.distributedcache.proxy.cluster.ClusterManager;
import com.iksanov.distributedcache.proxy.config.CacheClusterConfig;
import com.iksanov.distributedcache.proxy.metrics.CacheMetrics;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Service
public class CacheService {

    private static final Logger log = LoggerFactory.getLogger(CacheService.class);
    private final CacheClient client;
    private final ClusterManager clusterManager;
    private final CacheClusterConfig config;
    private final CacheMetrics metrics;

    public CacheService(CacheClient client, ClusterManager clusterManager, CacheClusterConfig config, CacheMetrics metrics) {
        this.client = client;
        this.clusterManager = clusterManager;
        this.config = config;
        this.metrics = metrics;
    }

    public String get(String key) {
        validateKey(key);
        CacheRequest request = new CacheRequest(
                generateRequestId(),
                System.currentTimeMillis(),
                CacheRequest.Command.GET,
                key,
                null
        );
        CacheResponse response = sendWithRetry(key, request, true);
        return switch (response.status()) {
            case OK -> response.value();
            case NOT_FOUND -> null;
            case ERROR -> throw new CacheConnectionException("Cache error: " + response.errorMessage());
        };
    }

    public void set(String key, String value) {
        validateKey(key);
        validateValue(value);
        CacheRequest request = new CacheRequest(
                generateRequestId(),
                System.currentTimeMillis(),
                CacheRequest.Command.SET,
                key,
                value
        );
        CacheResponse response = sendWithRetry(key, request, false);
        if (response.status() == CacheResponse.Status.ERROR) throw new CacheConnectionException("Failed to set key: " + response.errorMessage());
    }

    public void delete(String key) {
        validateKey(key);
        CacheRequest request = new CacheRequest(
                generateRequestId(),
                System.currentTimeMillis(),
                CacheRequest.Command.DELETE,
                key,
                null
        );
        CacheResponse response = sendWithRetry(key, request, false);
        if (response.status() == CacheResponse.Status.ERROR) throw new CacheConnectionException("Failed to delete key: " + response.errorMessage());
    }

    private CacheResponse sendWithRetry(String key, CacheRequest request, boolean isReadOperation) {
        int maxRetries = config.getConnection().getMaxRetries();
        int timeoutMillis = config.getConnection().getTimeoutMillis();
        CacheConnectionException lastException = null;
        Timer.Sample sample = metrics.startTimer();

        try {
            metrics.recordRequest();
            for (int attempt = 0; attempt <= maxRetries; attempt++) {
                try {
                    NodeInfo node = isReadOperation ? clusterManager.getNodeForRead(key) : clusterManager.getMasterNodeForKey(key);

                    log.debug("Sending {} request for key '{}' to {} node {} (attempt {}/{})",
                            request.command(), key,
                            isReadOperation ? "read" : "master",
                            node.nodeId(), attempt + 1, maxRetries + 1);

                    CompletableFuture<CacheResponse> future = client.sendRequest(node, request);
                    CacheResponse response = future.get(timeoutMillis, TimeUnit.MILLISECONDS);
                    log.debug("Received response for key '{}': status={}", key, response.status());
                    metrics.recordSuccess();
                    return response;
                } catch (TimeoutException e) {
                    lastException = new CacheConnectionException("Request timeout for key: " + key, e);
                    log.warn("Timeout on attempt {}/{} for key '{}'", attempt + 1, maxRetries + 1, key);
                } catch (Exception e) {
                    lastException = new CacheConnectionException("Request failed for key: " + key, e);
                    log.warn("Error on attempt {}/{} for key '{}': {}", attempt + 1, maxRetries + 1, key, e.getMessage());
                }
                if (attempt < maxRetries) {
                    metrics.recordRetry();
                    try {
                        Thread.sleep(100L * (attempt + 1));
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new CacheConnectionException("Interrupted during retry", ie);
                    }
                }
            }
            metrics.recordFailure();
            throw lastException;
        } finally {
            metrics.stopTimer(sample);
        }
    }

    private void validateKey(String key) {
        if (key == null || key.isBlank()) throw new InvalidCacheRequestException("Key cannot be null or empty");
        if (key.length() > 1000) throw new InvalidCacheRequestException("Key too long (max 1000 chars)");
    }

    private void validateValue(String value) {
        if (value == null) throw new InvalidCacheRequestException("Value cannot be null");
        if (value.length() > 10_000) throw new InvalidCacheRequestException("Value too long (max 10000 chars)");
    }

    private String generateRequestId() {
        return UUID.randomUUID().toString();
    }
}