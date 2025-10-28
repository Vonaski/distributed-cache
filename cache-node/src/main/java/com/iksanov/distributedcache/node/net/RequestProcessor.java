package com.iksanov.distributedcache.node.net;

import com.iksanov.distributedcache.common.dto.CacheRequest;
import com.iksanov.distributedcache.common.dto.CacheResponse;
import com.iksanov.distributedcache.common.exception.CacheException;
import com.iksanov.distributedcache.node.core.CacheStore;
import com.iksanov.distributedcache.node.replication.ReplicationManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

/**
 * RequestProcessor is responsible for processing {@link CacheRequest} objects
 * and producing corresponding {@link CacheResponse} results.
 * <p>
 * This class isolates the network layer (Netty) from the core cache logic (CacheStore).
 * It is thread-safe, production-ready, and designed for extension:
 *  - Validates incoming requests
 *  - Converts exceptions into CacheResponse errors
 *  - Logs performance and error details
 *  - Can be extended later with metrics, replication, or async processing
 */
public class RequestProcessor {

    private static final Logger log = LoggerFactory.getLogger(RequestProcessor.class);
    private final CacheStore store;
    private final ReplicationManager replicationManager;
    private static final long SLOW_REQUEST_THRESHOLD_MS = 100;

    public RequestProcessor(CacheStore store) {
        this(store, null);
    }

    public RequestProcessor(CacheStore store, ReplicationManager replicationManager) {
        this.store = Objects.requireNonNull(store, "store");
        this.replicationManager = replicationManager;
    }

    /**
     * Processes a single CacheRequest and returns a CacheResponse.
     * This method is synchronous and blocking but can be extended for async/offload later.
     *
     * @param request Incoming cache command request
     * @return A response object representing the result of the operation
     */
    public CacheResponse process(CacheRequest request) {
        Objects.requireNonNull(request, "request");

        Instant start = Instant.now();
        try {
            return switch (request.command()) {
                case GET -> handleGet(request);
                case SET -> handleSet(request);
                case DELETE -> handleDelete(request);
            };
        } catch (CacheException ce) {
            log.error("CacheException while processing requestId={}: {}", request.requestId(), ce.getMessage());
            return CacheResponse.error(request.requestId(), ce.getMessage());
        } catch (Exception e) {
            log.error("Unexpected error while processing requestId={}", request.requestId(), e);
            return CacheResponse.error(request.requestId(), "Internal server error");
        } finally {
            Duration duration = Duration.between(start, Instant.now());
            if (duration.toMillis() > SLOW_REQUEST_THRESHOLD_MS) {
                log.debug("Processed requestId={} command={} in {} ms", request.requestId(), request.command(), duration.toMillis());
            }
        }
    }

    private CacheResponse handleGet(CacheRequest request) {
        String key = requireKey(request);
        String value = store.get(key);
        if (value == null) return CacheResponse.notFound(request.requestId());
        return CacheResponse.ok(request.requestId(), value);
    }

    private CacheResponse handleSet(CacheRequest request) {
        String key = requireKey(request);
        String value = requireValue(request);
        store.put(key, value);
        if (replicationManager != null) {
            try {
                replicationManager.onLocalSet(key, value);
            } catch (Exception e) {
                log.warn("ReplicationManager.onLocalSet threw exception for key={}: {}", key, e.getMessage());
            }
        }
        return CacheResponse.ok(request.requestId(), "OK");
    }

    private CacheResponse handleDelete(CacheRequest request) {
        String key = requireKey(request);
        store.delete(key);
        if (replicationManager != null) {
            try {
                replicationManager.onLocalDelete(key);
            } catch (Exception e) {
                log.warn("ReplicationManager.onLocalDelete threw exception for key={}: {}", key, e.getMessage());
            }
        }
        return CacheResponse.ok(request.requestId(), "OK");
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
