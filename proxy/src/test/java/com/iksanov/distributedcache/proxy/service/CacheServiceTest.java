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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.lenient;

/**
 * Unit tests for {@link CacheService}.
 * <p>
 * Tests request routing, retry logic, and error handling.
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("CacheService Tests")
class CacheServiceTest {

    @Mock
    private CacheClient client;

    @Mock
    private ClusterManager clusterManager;

    @Mock
    private CacheClusterConfig config;

    @Mock
    private CacheClusterConfig.ConnectionConfig connectionConfig;

    @Mock
    private CacheMetrics metrics;

    private CacheService cacheService;

    private NodeInfo master1;
    private NodeInfo replica1;

    @BeforeEach
    void setUp() {
        lenient().when(config.getConnection()).thenReturn(connectionConfig);
        lenient().when(connectionConfig.getMaxRetries()).thenReturn(2);
        lenient().when(connectionConfig.getTimeoutMillis()).thenReturn(1000);
        cacheService = new CacheService(client, clusterManager, config, metrics);
        master1 = new NodeInfo("master-1", "localhost", 7000, 7100);
        replica1 = new NodeInfo("replica-1", "localhost", 7001, 7101);
        lenient().when(metrics.startTimer()).thenReturn(mock(io.micrometer.core.instrument.Timer.Sample.class));
    }

    @Test
    @DisplayName("Should successfully get value from cache")
    void shouldSuccessfullyGetValueFromCache() {
        String key = "user:1";
        String value = "Alice";
        when(clusterManager.getNodeForRead(key)).thenReturn(master1);

        CacheResponse response = new CacheResponse(
                "req-1",
                value,
                CacheResponse.Status.OK,
                null
        );
        when(client.sendRequest(eq(master1), any(CacheRequest.class))).thenReturn(CompletableFuture.completedFuture(response));
        String result = cacheService.get(key);
        assertEquals(value, result);
        verify(metrics).recordRequest();
        verify(metrics).recordSuccess();
        verify(client).sendRequest(eq(master1), any(CacheRequest.class));
    }

    @Test
    @DisplayName("Should return null when key not found")
    void shouldReturnNullWhenKeyNotFound() {
        String key = "user:999";
        when(clusterManager.getNodeForRead(key)).thenReturn(master1);
        CacheResponse response = new CacheResponse(
                "req-1",
                null,
                CacheResponse.Status.NOT_FOUND,
                null
        );
        when(client.sendRequest(eq(master1), any(CacheRequest.class))).thenReturn(CompletableFuture.completedFuture(response));
        String result = cacheService.get(key);
        assertNull(result);
        verify(metrics).recordSuccess();
    }

    @Test
    @DisplayName("Should throw exception on cache error")
    void shouldThrowExceptionOnCacheError() {
        String key = "user:1";
        when(clusterManager.getNodeForRead(key)).thenReturn(master1);
        CacheResponse response = new CacheResponse(
                "req-1",
                null,
                CacheResponse.Status.ERROR,
                "Internal server error"
        );
        when(client.sendRequest(eq(master1), any(CacheRequest.class))).thenReturn(CompletableFuture.completedFuture(response));
        assertThrows(CacheConnectionException.class, () -> cacheService.get(key));
    }

    @Test
    @DisplayName("Should successfully set value in cache")
    void shouldSuccessfullySetValueInCache() {
        String key = "user:1";
        String value = "Alice";
        when(clusterManager.getMasterNodeForKey(key)).thenReturn(master1);
        CacheResponse response = new CacheResponse(
                "req-1",
                null,
                CacheResponse.Status.OK,
                null
        );

        when(client.sendRequest(eq(master1), any(CacheRequest.class))).thenReturn(CompletableFuture.completedFuture(response));
        assertDoesNotThrow(() -> cacheService.set(key, value));
        verify(metrics).recordRequest();
        verify(metrics).recordSuccess();
        ArgumentCaptor<CacheRequest> requestCaptor = ArgumentCaptor.forClass(CacheRequest.class);
        verify(client).sendRequest(eq(master1), requestCaptor.capture());
        CacheRequest capturedRequest = requestCaptor.getValue();
        assertEquals(CacheRequest.Command.SET, capturedRequest.command());
        assertEquals(key, capturedRequest.key());
        assertEquals(value, capturedRequest.value());
    }

    @Test
    @DisplayName("Should successfully delete value from cache")
    void shouldSuccessfullyDeleteValueFromCache() {
        String key = "user:1";
        when(clusterManager.getMasterNodeForKey(key)).thenReturn(master1);
        CacheResponse response = new CacheResponse(
                "req-1",
                null,
                CacheResponse.Status.OK,
                null
        );
        when(client.sendRequest(eq(master1), any(CacheRequest.class))).thenReturn(CompletableFuture.completedFuture(response));
        assertDoesNotThrow(() -> cacheService.delete(key));
        ArgumentCaptor<CacheRequest> requestCaptor = ArgumentCaptor.forClass(CacheRequest.class);
        verify(client).sendRequest(eq(master1), requestCaptor.capture());
        CacheRequest capturedRequest = requestCaptor.getValue();
        assertEquals(CacheRequest.Command.DELETE, capturedRequest.command());
        assertEquals(key, capturedRequest.key());
        assertNull(capturedRequest.value());
    }

    @Test
    @DisplayName("Should retry on timeout and succeed")
    void shouldRetryOnTimeoutAndSucceed() {
        String key = "user:1";
        when(clusterManager.getNodeForRead(key)).thenReturn(master1);
        CompletableFuture<CacheResponse> timeoutFuture = new CompletableFuture<>();
        timeoutFuture.completeExceptionally(new TimeoutException());

        CacheResponse successResponse = new CacheResponse(
                "req-1",
                "Alice",
                CacheResponse.Status.OK,
                null
        );

        when(client.sendRequest(eq(master1), any(CacheRequest.class))).thenReturn(timeoutFuture).thenReturn(CompletableFuture.completedFuture(successResponse));
        String result = cacheService.get(key);
        assertEquals("Alice", result);
        verify(client, times(2)).sendRequest(eq(master1), any(CacheRequest.class));
        verify(metrics).recordRetry();
        verify(metrics).recordSuccess();
    }

    @Test
    @DisplayName("Should fail after max retries")
    void shouldFailAfterMaxRetries() {
        String key = "user:1";
        when(clusterManager.getNodeForRead(key)).thenReturn(master1);
        CompletableFuture<CacheResponse> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(new RuntimeException("Connection refused"));
        when(client.sendRequest(eq(master1), any(CacheRequest.class))).thenReturn(failedFuture);
        assertThrows(CacheConnectionException.class, () -> cacheService.get(key));
        verify(client, times(3)).sendRequest(eq(master1), any(CacheRequest.class));
        verify(metrics, times(2)).recordRetry();
        verify(metrics).recordFailure();
    }

    @Test
    @DisplayName("Should validate null key")
    void shouldValidateNullKey() {
        assertThrows(InvalidCacheRequestException.class, () -> cacheService.get(null));
        assertThrows(InvalidCacheRequestException.class, () -> cacheService.set(null, "value"));
        assertThrows(InvalidCacheRequestException.class, () -> cacheService.delete(null));
    }

    @Test
    @DisplayName("Should validate empty key")
    void shouldValidateEmptyKey() {
        assertThrows(InvalidCacheRequestException.class, () -> cacheService.get(""));
        assertThrows(InvalidCacheRequestException.class, () -> cacheService.get("   "));
        assertThrows(InvalidCacheRequestException.class, () -> cacheService.set("", "value"));
    }

    @Test
    @DisplayName("Should validate key length")
    void shouldValidateKeyLength() {
        String longKey = "x".repeat(1001);
        assertThrows(InvalidCacheRequestException.class, () -> cacheService.get(longKey));
        assertThrows(InvalidCacheRequestException.class, () -> cacheService.set(longKey, "value"));
    }

    @Test
    @DisplayName("Should validate null value in set")
    void shouldValidateNullValueInSet() {
        assertThrows(InvalidCacheRequestException.class, () -> cacheService.set("key", null));
    }

    @Test
    @DisplayName("Should validate value length")
    void shouldValidateValueLength() {
        String longValue = "x".repeat(10001);
        assertThrows(InvalidCacheRequestException.class, () -> cacheService.set("key", longValue));
    }

    @Test
    @DisplayName("Should use master node for write operations")
    void shouldUseMasterNodeForWriteOperations() {
        String key = "user:1";
        when(clusterManager.getMasterNodeForKey(key)).thenReturn(master1);
        CacheResponse response = new CacheResponse(
                "req-1",
                null,
                CacheResponse.Status.OK,
                null
        );
        when(client.sendRequest(eq(master1), any(CacheRequest.class))).thenReturn(CompletableFuture.completedFuture(response));
        cacheService.set(key, "value");
        verify(clusterManager).getMasterNodeForKey(key);
        verify(clusterManager, never()).getNodeForRead(key);
    }

    @Test
    @DisplayName("Should use read node for read operations")
    void shouldUseReadNodeForReadOperations() {
        String key = "user:1";
        when(clusterManager.getNodeForRead(key)).thenReturn(replica1);
        CacheResponse response = new CacheResponse(
                "req-1",
                "value",
                CacheResponse.Status.OK,
                null
        );
        when(client.sendRequest(eq(replica1), any(CacheRequest.class))).thenReturn(CompletableFuture.completedFuture(response));
        cacheService.get(key);
        verify(clusterManager).getNodeForRead(key);
        verify(clusterManager, never()).getMasterNodeForKey(key);
    }

    @Test
    @DisplayName("Should generate unique request IDs")
    void shouldGenerateUniqueRequestIds() {
        String key = "user:1";
        when(clusterManager.getNodeForRead(key)).thenReturn(master1);
        CacheResponse response = new CacheResponse(
                "req-1",
                "value",
                CacheResponse.Status.OK,
                null
        );
        when(client.sendRequest(eq(master1), any(CacheRequest.class))).thenReturn(CompletableFuture.completedFuture(response));
        cacheService.get(key);
        cacheService.get(key);
        ArgumentCaptor<CacheRequest> requestCaptor = ArgumentCaptor.forClass(CacheRequest.class);
        verify(client, times(2)).sendRequest(eq(master1), requestCaptor.capture());
        CacheRequest req1 = requestCaptor.getAllValues().get(0);
        CacheRequest req2 = requestCaptor.getAllValues().get(1);
        assertNotNull(req1.requestId());
        assertNotNull(req2.requestId());
        assertNotEquals(req1.requestId(), req2.requestId());
    }

    @Test
    @DisplayName("Should throw exception when SET fails")
    void shouldThrowExceptionWhenSetFails() {
        String key = "user:1";
        when(clusterManager.getMasterNodeForKey(key)).thenReturn(master1);
        CacheResponse errorResponse = new CacheResponse(
                "req-1",
                null,
                CacheResponse.Status.ERROR,
                "Write failed"
        );
        when(client.sendRequest(eq(master1), any(CacheRequest.class))).thenReturn(CompletableFuture.completedFuture(errorResponse));
        assertThrows(CacheConnectionException.class, () -> cacheService.set(key, "value"));
    }

    @Test
    @DisplayName("Should throw exception when DELETE fails")
    void shouldThrowExceptionWhenDeleteFails() {
        String key = "user:1";
        when(clusterManager.getMasterNodeForKey(key)).thenReturn(master1);
        CacheResponse errorResponse = new CacheResponse(
                "req-1",
                null,
                CacheResponse.Status.ERROR,
                "Delete failed"
        );
        when(client.sendRequest(eq(master1), any(CacheRequest.class))).thenReturn(CompletableFuture.completedFuture(errorResponse));
        assertThrows(CacheConnectionException.class, () -> cacheService.delete(key));
    }

    @Test
    @DisplayName("Should record metrics for all operations")
    void shouldRecordMetricsForAllOperations() {
        String key = "user:1";
        when(clusterManager.getMasterNodeForKey(key)).thenReturn(master1);
        when(clusterManager.getNodeForRead(key)).thenReturn(master1);
        CacheResponse response = new CacheResponse(
                "req-1",
                "value",
                CacheResponse.Status.OK,
                null
        );
        when(client.sendRequest(eq(master1), any(CacheRequest.class))).thenReturn(CompletableFuture.completedFuture(response));
        cacheService.get(key);
        cacheService.set(key, "value");
        cacheService.delete(key);
        verify(metrics, times(3)).recordRequest();
        verify(metrics, times(3)).recordSuccess();
        verify(metrics, times(3)).startTimer();
        verify(metrics, times(3)).stopTimer(any());
    }

    @Test
    @DisplayName("Should handle interrupted exception during retry sleep")
    void shouldHandleInterruptedExceptionDuringRetrySleep() {
        String key = "user:1";
        when(clusterManager.getNodeForRead(key)).thenReturn(master1);
        CompletableFuture<CacheResponse> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(new RuntimeException("Connection refused"));
        when(client.sendRequest(eq(master1), any(CacheRequest.class))).thenReturn(failedFuture);
        Thread.currentThread().interrupt();
        assertThrows(CacheConnectionException.class, () -> cacheService.get(key));
        assertTrue(Thread.interrupted());
    }
}