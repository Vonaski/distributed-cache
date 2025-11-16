package com.iksanov.distributedcache.node.net;

import com.iksanov.distributedcache.common.dto.CacheRequest;
import com.iksanov.distributedcache.common.dto.CacheResponse;
import com.iksanov.distributedcache.common.exception.CacheException;
import com.iksanov.distributedcache.node.config.NetServerConfig;
import com.iksanov.distributedcache.node.consensus.model.Command;
import com.iksanov.distributedcache.node.consensus.raft.RaftNode;
import com.iksanov.distributedcache.node.consensus.raft.RaftStateMachine;
import com.iksanov.distributedcache.node.consensus.sharding.ShardManager;
import com.iksanov.distributedcache.node.metrics.NetMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * RequestProcessor is responsible for processing {@link CacheRequest} objects
 * and producing corresponding {@link CacheResponse} results.
 * <p>
 * This version integrates with Raft consensus:
 *  - GET requests read from committed state via RaftStateMachine
 *  - SET/DELETE requests go through Raft consensus for replication
 *  - Uses ShardManager to determine which shard handles each key
 *  - Validates incoming requests
 *  - Converts exceptions into CacheResponse errors
 *  - Logs performance and error details
 */
public class RequestProcessor {

    private static final Logger log = LoggerFactory.getLogger(RequestProcessor.class);
    private final NetMetrics metrics;
    private final ShardManager shardManager;
    private final NetServerConfig config;

    public RequestProcessor(ShardManager shardManager, NetMetrics metrics, NetServerConfig config) {
        this.shardManager = Objects.requireNonNull(shardManager, "shardManager cannot be null");
        this.metrics = Objects.requireNonNull(metrics, "metrics cannot be null");
        this.config = Objects.requireNonNull(config, "config cannot be null");
    }

    public CacheResponse process(CacheRequest request) {
        Objects.requireNonNull(request, "request");
        Instant start = Instant.now();
        metrics.incrementRequests();
        try {
            return switch (request.command()) {
                case GET -> handleGet(request);
                case SET -> handleSet(request);
                case DELETE -> handleDelete(request);
            };
        } catch (CacheException ce) {
            metrics.incrementErrors();
            log.error("CacheException while processing requestId={}: {}", request.requestId(), ce.getMessage());
            return CacheResponse.error(request.requestId(), ce.getMessage());
        } catch (Exception e) {
            metrics.incrementErrors();
            log.error("Unexpected error while processing requestId={}", request.requestId(), e);
            return CacheResponse.error(request.requestId(), "Internal server error");
        } finally {
            long durationMs = Duration.between(start, Instant.now()).toMillis();
            metrics.recordRequestDuration(durationMs);
            metrics.incrementResponses();
            if (durationMs > config.slowRequestThresholdMs()) {
                log.warn("Slow request [command={}, requestId={}] took {} ms", request.command(), request.requestId(), durationMs);
            } else {
                log.debug("Request [command={}, requestId={}] processed in {} ms", request.command(), request.requestId(), durationMs);
            }
        }
    }

    private CacheResponse handleGet(CacheRequest request) {
        String key = requireKey(request);
        String shardId = shardManager.selectShardForKey(key);
        if (shardId == null) {
            log.warn("No shard found for key={}", key);
            return CacheResponse.error(request.requestId(), "No shard available for key");
        }
        RaftNode raftNode = shardManager.getShard(shardId);
        if (raftNode == null) {
            log.warn("RaftNode not found for shard={}, key={}", shardId, key);
            return CacheResponse.error(request.requestId(), "Shard not available locally");
        }
        RaftStateMachine stateMachine = shardManager.getStateMachine(shardId);
        if (stateMachine == null) {
            log.error("StateMachine not available for shard={}", shardId);
            return CacheResponse.error(request.requestId(), "State machine not available");
        }
        String value = stateMachine.get(key);
        if (value == null) {
            log.debug("GET miss for key={} in shard={}", key, shardId);
            return CacheResponse.notFound(request.requestId());
        }
        log.trace("GET hit for key={} in shard={} valueLength={}", key, shardId, value.length());
        return CacheResponse.ok(request.requestId(), value);
    }

    private CacheResponse handleSet(CacheRequest request) {
        String key = requireKey(request);
        String value = requireValue(request);
        String shardId = shardManager.selectShardForKey(key);
        if (shardId == null) {
            log.warn("No shard found for key={}", key);
            return CacheResponse.error(request.requestId(), "No shard available for key");
        }
        RaftNode raftNode = shardManager.getShard(shardId);
        if (raftNode == null) {
            log.warn("RaftNode not found for shard={}, key={}", shardId, key);
            return CacheResponse.error(request.requestId(), "Shard not available locally");
        }
        Command command = Command.set(key, value);
        CompletableFuture<Long> future = raftNode.submit(command);
        try {
            Long commitIndex = future.get(config.consensusTimeoutSeconds(), TimeUnit.SECONDS);
            log.debug("SET key={} committed at index={} in shard={}", key, commitIndex, shardId);
            return CacheResponse.ok(request.requestId(), "OK");
        } catch (Exception e) {
            metrics.incrementErrors();
            log.error("Failed to commit SET key={} in shard={}: {}", key, shardId, e.getMessage(), e);
            if (e.getCause() != null && e.getCause() instanceof IllegalStateException) {
                return CacheResponse.error(request.requestId(), "Not leader: " + e.getCause().getMessage());
            }
            return CacheResponse.error(request.requestId(), "Consensus failed: " + e.getMessage());
        }
    }

    private CacheResponse handleDelete(CacheRequest request) {
        String key = requireKey(request);
        String shardId = shardManager.selectShardForKey(key);
        if (shardId == null) {
            log.warn("No shard found for key={}", key);
            return CacheResponse.error(request.requestId(), "No shard available for key");
        }
        RaftNode raftNode = shardManager.getShard(shardId);
        if (raftNode == null) {
            log.warn("RaftNode not found for shard={}, key={}", shardId, key);
            return CacheResponse.error(request.requestId(), "Shard not available locally");
        }
        Command command = Command.delete(key);
        CompletableFuture<Long> future = raftNode.submit(command);

        try {
            Long commitIndex = future.get(config.consensusTimeoutSeconds(), TimeUnit.SECONDS);
            log.debug("DELETE key={} committed at index={} in shard={}", key, commitIndex, shardId);
            return CacheResponse.ok(request.requestId(), "OK");
        } catch (Exception e) {
            metrics.incrementErrors();
            log.error("Failed to commit DELETE key={} in shard={}: {}", key, shardId, e.getMessage(), e);
            if (e.getCause() != null && e.getCause() instanceof IllegalStateException) {
                return CacheResponse.error(request.requestId(), "Not leader: " + e.getCause().getMessage());
            }
            return CacheResponse.error(request.requestId(), "Consensus failed: " + e.getMessage());
        }
    }

    private String requireKey(CacheRequest request) {
        String key = request.key();
        if (key == null || key.isBlank()) throw new CacheException("Key must not be null or blank");
        return key;
    }

    private String requireValue(CacheRequest request) {
        String value = request.value();
        if (value == null) throw new CacheException("Value must not be null for SET");
        return value;
    }
}
