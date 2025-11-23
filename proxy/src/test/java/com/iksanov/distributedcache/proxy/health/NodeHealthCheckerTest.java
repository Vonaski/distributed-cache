package com.iksanov.distributedcache.proxy.health;

import com.iksanov.distributedcache.common.cluster.NodeInfo;
import com.iksanov.distributedcache.common.dto.CacheRequest;
import com.iksanov.distributedcache.common.dto.CacheResponse;
import com.iksanov.distributedcache.proxy.client.CacheClient;
import com.iksanov.distributedcache.proxy.cluster.ClusterManager;
import com.iksanov.distributedcache.proxy.metrics.CacheMetrics;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link NodeHealthChecker}.
 * <p>
 * Tests health checking, failover logic, and metrics integration.
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("NodeHealthChecker Tests")
class NodeHealthCheckerTest {

    @Mock
    private ClusterManager clusterManager;

    @Mock
    private CacheClient cacheClient;

    @Mock
    private CacheMetrics metrics;

    private NodeHealthChecker healthChecker;

    private NodeInfo master1;
    private NodeInfo master2;
    private NodeInfo replica1;
    private NodeInfo replica2;

    @BeforeEach
    void setUp() {
        healthChecker = new NodeHealthChecker(clusterManager, cacheClient, metrics);
        master1 = new NodeInfo("master-1", "localhost", 7000, 7100);
        master2 = new NodeInfo("master-2", "localhost", 7001, 7101);
        replica1 = new NodeInfo("replica-1", "localhost", 7002, 7102);
        replica2 = new NodeInfo("replica-2", "localhost", 7003, 7103);
    }

    @Test
    @DisplayName("Should skip health check when no masters configured")
    void shouldSkipHealthCheckWhenNoMasters() {
        when(clusterManager.getAllNodes()).thenReturn(Set.of());
        healthChecker.checkClusterHealth();
        verify(cacheClient, never()).sendRequest(any(), any());
        verify(metrics, never()).recordHealthCheckFailure();
    }

    @Test
    @DisplayName("Should successfully ping healthy master using STATUS command")
    void shouldSuccessfullyPingHealthyMaster() {
        when(clusterManager.getAllNodes()).thenReturn(Set.of(master1));
        CacheResponse healthyResponse = new CacheResponse(
                "req-1",
                null,
                CacheResponse.Status.OK,
                null,
                "role:MASTER,epoch:0"
        );

        when(cacheClient.sendRequest(eq(master1), any(CacheRequest.class))).thenReturn(CompletableFuture.completedFuture(healthyResponse));
        healthChecker.checkClusterHealth();
        verify(cacheClient).sendRequest(eq(master1), any(CacheRequest.class));
        verify(clusterManager, never()).removeMaster(any());
        verify(metrics, never()).recordHealthCheckFailure();
        assertEquals(0, healthChecker.getFailureCount("master-1"));
        assertFalse(healthChecker.isMarkedAsFailed("master-1"));
    }

    @Test
    @DisplayName("Should record failure but not trigger failover on first failure")
    void shouldRecordFailureButNotTriggerFailoverOnFirstFailure() {
        when(clusterManager.getAllNodes()).thenReturn(Set.of(master1));
        when(cacheClient.sendRequest(eq(master1), any(CacheRequest.class))).thenReturn(CompletableFuture.failedFuture(new RuntimeException("Connection refused")));
        healthChecker.checkClusterHealth();
        verify(metrics).recordHealthCheckFailure();
        verify(clusterManager, never()).removeMaster(any());
        verify(metrics, never()).recordFailover();
        assertEquals(1, healthChecker.getFailureCount("master-1"));
        assertFalse(healthChecker.isMarkedAsFailed("master-1"));
    }

    @Test
    @DisplayName("Should trigger failover after 2 consecutive failures and send PROMOTE with replica list")
    void shouldTriggerFailoverAfterConsecutiveFailures() {
        NodeInfo replica2ForMaster1 = new NodeInfo("replica-2-master-1", "localhost", 7004, 7104);
        when(clusterManager.getAllNodes()).thenReturn(Set.of(master1));
        when(clusterManager.getReplicas(master1)).thenReturn(Set.of(replica1, replica2ForMaster1));
        when(cacheClient.sendRequest(eq(master1), any(CacheRequest.class))).thenReturn(CompletableFuture.failedFuture(new RuntimeException("Connection refused")));
        CacheResponse statusResponse = new CacheResponse(
                "status-1",
                null,
                CacheResponse.Status.OK,
                null,
                "role:REPLICA,epoch:0"
        );

        CacheResponse promoteResponse = new CacheResponse(
                "promote-1",
                null,
                CacheResponse.Status.OK,
                null,
                "role:MASTER,epoch:123"
        );

        when(cacheClient.sendRequest(eq(replica1), any(CacheRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(statusResponse))
                .thenReturn(CompletableFuture.completedFuture(promoteResponse));

        healthChecker.checkClusterHealth();
        assertEquals(1, healthChecker.getFailureCount("master-1"));
        verify(clusterManager, never()).removeMaster(any());
        healthChecker.checkClusterHealth();
        verify(metrics, times(2)).recordHealthCheckFailure();
        verify(clusterManager).removeMaster(master1);
        verify(clusterManager).addMaster(replica1);
        verify(metrics).recordFailover();
        ArgumentCaptor<CacheRequest> requestCaptor = ArgumentCaptor.forClass(CacheRequest.class);
        verify(cacheClient, atLeast(2)).sendRequest(eq(replica1), requestCaptor.capture());
        CacheRequest promoteRequest = requestCaptor.getAllValues().stream()
                .filter(req -> req.command() == CacheRequest.Command.PROMOTE)
                .findFirst()
                .orElse(null);

        assertNotNull(promoteRequest, "PROMOTE command should be sent");
        assertTrue(promoteRequest.value().contains("epoch:"), "Should contain epoch");
        assertTrue(promoteRequest.value().contains("replicas:"), "Should contain replicas list");
        assertTrue(promoteRequest.value().contains("replica-2-master-1"), "Should include replica-2");
        assertTrue(healthChecker.isMarkedAsFailed("master-1"));
    }

    @Test
    @DisplayName("Should skip already failed masters in health check")
    void shouldSkipAlreadyFailedMasters() {
        when(clusterManager.getAllNodes()).thenReturn(Set.of(master1));
        when(clusterManager.getReplicas(master1)).thenReturn(Set.of(replica1));
        when(cacheClient.sendRequest(eq(master1), any(CacheRequest.class))).thenReturn(CompletableFuture.failedFuture(new RuntimeException("Connection refused")));
        CacheResponse statusResponse = new CacheResponse(
                "req-1",
                null,
                CacheResponse.Status.OK,
                null,
                "role:REPLICA,epoch:0"
        );
        CacheResponse promoteResponse = new CacheResponse(
                "promote-1",
                null,
                CacheResponse.Status.OK,
                null,
                "role:MASTER,epoch:123"
        );
        when(cacheClient.sendRequest(eq(replica1), any(CacheRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(statusResponse))
                .thenReturn(CompletableFuture.completedFuture(promoteResponse));

        healthChecker.checkClusterHealth();
        healthChecker.checkClusterHealth();
        assertTrue(healthChecker.isMarkedAsFailed("master-1"));
        healthChecker.checkClusterHealth();
        verify(clusterManager, times(1)).removeMaster(master1);
    }

    @Test
    @DisplayName("Should handle timeout as failure")
    void shouldHandleTimeoutAsFailure() {
        when(clusterManager.getAllNodes()).thenReturn(Set.of(master1));
        CompletableFuture<CacheResponse> timeoutFuture = new CompletableFuture<>();
        timeoutFuture.completeExceptionally(new TimeoutException("Request timeout"));
        when(cacheClient.sendRequest(eq(master1), any(CacheRequest.class))).thenReturn(timeoutFuture);
        healthChecker.checkClusterHealth();
        verify(metrics).recordHealthCheckFailure();
        assertEquals(1, healthChecker.getFailureCount("master-1"));
    }

    @Test
    @DisplayName("Should fail failover when no replicas available")
    void shouldFailFailoverWhenNoReplicasAvailable() {
        when(clusterManager.getAllNodes()).thenReturn(Set.of(master1));
        when(clusterManager.getReplicas(master1)).thenReturn(Set.of());
        when(cacheClient.sendRequest(eq(master1), any(CacheRequest.class))).thenReturn(CompletableFuture.failedFuture(new RuntimeException("Connection refused")));
        healthChecker.checkClusterHealth();
        healthChecker.checkClusterHealth();
        verify(clusterManager).removeMaster(master1);
        verify(clusterManager, never()).addMaster(any());
        verify(metrics, never()).recordFailover();
        assertTrue(healthChecker.isMarkedAsFailed("master-1"));
    }

    @Test
    @DisplayName("Should try all replicas and promote first healthy one")
    void shouldTryAllReplicasAndPromoteFirstHealthy() {
        when(clusterManager.getAllNodes()).thenReturn(Set.of(master1));
        when(clusterManager.getReplicas(master1)).thenReturn(Set.of(replica1, replica2));
        when(cacheClient.sendRequest(eq(master1), any(CacheRequest.class))).thenReturn(CompletableFuture.failedFuture(new RuntimeException("Connection refused")));
        when(cacheClient.sendRequest(eq(replica1), any(CacheRequest.class))).thenReturn(CompletableFuture.failedFuture(new RuntimeException("Connection refused")));

        CacheResponse statusResponse = new CacheResponse(
                "req-1",
                null,
                CacheResponse.Status.OK,
                null,
                "role:REPLICA,epoch:0"
        );

        CacheResponse promoteResponse = new CacheResponse(
                "promote-1",
                null,
                CacheResponse.Status.OK,
                null,
                "role:MASTER,epoch:123"
        );

        when(cacheClient.sendRequest(eq(replica2), any(CacheRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(statusResponse))
                .thenReturn(CompletableFuture.completedFuture(promoteResponse));

        healthChecker.checkClusterHealth();
        healthChecker.checkClusterHealth();
        verify(clusterManager).removeMaster(master1);
        verify(clusterManager).addMaster(replica2);
        verify(metrics).recordFailover();
    }

    @Test
    @DisplayName("Should fail failover when all replicas are unhealthy")
    void shouldFailFailoverWhenAllReplicasUnhealthy() {
        when(clusterManager.getAllNodes()).thenReturn(Set.of(master1));
        when(clusterManager.getReplicas(master1)).thenReturn(Set.of(replica1, replica2));
        when(cacheClient.sendRequest(any(), any(CacheRequest.class))).thenReturn(CompletableFuture.failedFuture(new RuntimeException("Connection refused")));
        healthChecker.checkClusterHealth();
        healthChecker.checkClusterHealth();
        verify(clusterManager).removeMaster(master1);
        verify(clusterManager, never()).addMaster(any());
        verify(metrics, never()).recordFailover();
        assertTrue(healthChecker.isMarkedAsFailed("master-1"));
    }

    @Test
    @DisplayName("Should reset failure count after successful ping")
    void shouldResetFailureCountAfterSuccessfulPing() {
        when(clusterManager.getAllNodes()).thenReturn(Set.of(master1));
        CacheResponse healthyResponse = new CacheResponse(
                "req-1",
                null,
                CacheResponse.Status.OK,
                null,
                "role:MASTER,epoch:100"
        );

        when(cacheClient.sendRequest(eq(master1), any(CacheRequest.class))).thenReturn(CompletableFuture.failedFuture(new RuntimeException("Connection refused")));
        healthChecker.checkClusterHealth();
        assertEquals(1, healthChecker.getFailureCount("master-1"));
        when(cacheClient.sendRequest(eq(master1), any(CacheRequest.class))).thenReturn(CompletableFuture.completedFuture(healthyResponse));
        healthChecker.checkClusterHealth();
        assertEquals(0, healthChecker.getFailureCount("master-1"));
        assertFalse(healthChecker.isMarkedAsFailed("master-1"));
    }

    @Test
    @DisplayName("Should manually trigger health check")
    void shouldManuallyTriggerHealthCheck() {
        when(clusterManager.getAllNodes()).thenReturn(Set.of(master1));

        CacheResponse healthyResponse = new CacheResponse(
                "req-1",
                null,
                CacheResponse.Status.OK,
                null,
                "role:MASTER,epoch:100"
        );
        when(cacheClient.sendRequest(eq(master1), any(CacheRequest.class))).thenReturn(CompletableFuture.completedFuture(healthyResponse));
        healthChecker.triggerHealthCheck();
        verify(cacheClient).sendRequest(eq(master1), any(CacheRequest.class));
    }

    @Test
    @DisplayName("Should reset failure tracking manually")
    void shouldResetFailureTrackingManually() {
        when(clusterManager.getAllNodes()).thenReturn(Set.of(master1));
        when(cacheClient.sendRequest(eq(master1), any(CacheRequest.class))).thenReturn(CompletableFuture.failedFuture(new RuntimeException("Connection refused")));
        healthChecker.checkClusterHealth();
        assertEquals(1, healthChecker.getFailureCount("master-1"));
        healthChecker.resetFailureTracking("master-1");
        assertEquals(0, healthChecker.getFailureCount("master-1"));
        assertFalse(healthChecker.isMarkedAsFailed("master-1"));
    }

    @Test
    @DisplayName("Should accept OK status with valid metadata as healthy")
    void shouldAcceptOkStatusWithValidMetadata() {
        when(clusterManager.getAllNodes()).thenReturn(Set.of(master1, master2));
        CacheResponse okResponse = new CacheResponse(
                "req-1",
                null,
                CacheResponse.Status.OK,
                null,
                "role:MASTER,epoch:100"
        );
        when(cacheClient.sendRequest(eq(master1), any(CacheRequest.class))).thenReturn(CompletableFuture.completedFuture(okResponse));
        CacheResponse okResponse2 = new CacheResponse(
                "req-2",
                null,
                CacheResponse.Status.OK,
                null,
                "role:MASTER,epoch:200"
        );
        when(cacheClient.sendRequest(eq(master2), any(CacheRequest.class))).thenReturn(CompletableFuture.completedFuture(okResponse2));
        healthChecker.checkClusterHealth();
        verify(metrics, never()).recordHealthCheckFailure();
        assertEquals(0, healthChecker.getFailureCount("master-1"));
        assertEquals(0, healthChecker.getFailureCount("master-2"));
    }

    @Test
    @DisplayName("Should treat ERROR status as unhealthy")
    void shouldTreatErrorStatusAsUnhealthy() {
        when(clusterManager.getAllNodes()).thenReturn(Set.of(master1));
        CacheResponse errorResponse = new CacheResponse(
                "req-1",
                null,
                CacheResponse.Status.ERROR,
                "Internal error"
        );
        when(cacheClient.sendRequest(eq(master1), any(CacheRequest.class))).thenReturn(CompletableFuture.completedFuture(errorResponse));
        healthChecker.checkClusterHealth();
        verify(metrics).recordHealthCheckFailure();
        assertEquals(1, healthChecker.getFailureCount("master-1"));
    }

    @Test
    @DisplayName("Should use STATUS command for health checks")
    void shouldUseStatusCommandForHealthChecks() {
        when(clusterManager.getAllNodes()).thenReturn(Set.of(master1));

        CacheResponse healthyResponse = new CacheResponse(
                "req-1",
                null,
                CacheResponse.Status.OK,
                null,
                "role:MASTER,epoch:100"
        );
        when(cacheClient.sendRequest(eq(master1), any(CacheRequest.class))).thenReturn(CompletableFuture.completedFuture(healthyResponse));
        healthChecker.checkClusterHealth();
        ArgumentCaptor<CacheRequest> requestCaptor = ArgumentCaptor.forClass(CacheRequest.class);
        verify(cacheClient).sendRequest(eq(master1), requestCaptor.capture());
        CacheRequest capturedRequest = requestCaptor.getValue();
        assertEquals(CacheRequest.Command.STATUS, capturedRequest.command());
        assertNull(capturedRequest.key());
        assertNull(capturedRequest.value());
    }

    @Test
    @DisplayName("Should detect already self-promoted replica and skip PROMOTE")
    void shouldDetectAlreadySelfPromotedReplica() {
        when(clusterManager.getAllNodes()).thenReturn(Set.of(master1));
        when(clusterManager.getReplicas(master1)).thenReturn(Set.of(replica1));
        when(cacheClient.sendRequest(eq(master1), any(CacheRequest.class))).thenReturn(CompletableFuture.failedFuture(new RuntimeException("Connection refused")));
        CacheResponse alreadyMasterResponse = new CacheResponse(
                "status-1",
                null,
                CacheResponse.Status.OK,
                null,
                "role:MASTER,epoch:999"
        );
        when(cacheClient.sendRequest(eq(replica1), any(CacheRequest.class))).thenReturn(CompletableFuture.completedFuture(alreadyMasterResponse));
        healthChecker.checkClusterHealth();
        healthChecker.checkClusterHealth();
        ArgumentCaptor<CacheRequest> requestCaptor = ArgumentCaptor.forClass(CacheRequest.class);
        verify(cacheClient, atLeastOnce()).sendRequest(eq(replica1), requestCaptor.capture());
        long promoteCount = requestCaptor.getAllValues().stream()
                .filter(req -> req.command() == CacheRequest.Command.PROMOTE)
                .count();
        assertEquals(0, promoteCount, "Should not send PROMOTE if replica already self-promoted");
        verify(clusterManager).removeMaster(master1);
        verify(clusterManager).addMaster(replica1);
        verify(metrics).recordFailover();
    }

    @Test
    @DisplayName("Should send PROMOTE with empty replica list when only one replica exists")
    void shouldSendPromoteWithEmptyReplicaListWhenOnlyOneReplica() {
        when(clusterManager.getAllNodes()).thenReturn(Set.of(master1));
        when(clusterManager.getReplicas(master1)).thenReturn(Set.of(replica1));
        when(cacheClient.sendRequest(eq(master1), any(CacheRequest.class))).thenReturn(CompletableFuture.failedFuture(new RuntimeException("Connection refused")));
        CacheResponse statusResponse = new CacheResponse(
                "status-1",
                null,
                CacheResponse.Status.OK,
                null,
                "role:REPLICA,epoch:0"
        );

        CacheResponse promoteResponse = new CacheResponse(
                "promote-1",
                null,
                CacheResponse.Status.OK,
                null,
                "role:MASTER,epoch:123"
        );

        when(cacheClient.sendRequest(eq(replica1), any(CacheRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(statusResponse))
                .thenReturn(CompletableFuture.completedFuture(promoteResponse));
        healthChecker.checkClusterHealth();
        healthChecker.checkClusterHealth();
        ArgumentCaptor<CacheRequest> requestCaptor = ArgumentCaptor.forClass(CacheRequest.class);
        verify(cacheClient, atLeast(2)).sendRequest(eq(replica1), requestCaptor.capture());
        CacheRequest promoteRequest = requestCaptor.getAllValues().stream()
                .filter(req -> req.command() == CacheRequest.Command.PROMOTE)
                .findFirst()
                .orElse(null);
        assertNotNull(promoteRequest);
        assertTrue(promoteRequest.value().startsWith("epoch:"));
        assertFalse(promoteRequest.value().contains("replicas:"), "Should not contain replicas when only 1 replica");
    }

    @Test
    @DisplayName("Should send PROMOTE with multiple replicas in list")
    void shouldSendPromoteWithMultipleReplicasInList() {
        NodeInfo replica1Master1 = new NodeInfo("replica-1-master-1", "localhost", 7003, 7103);
        NodeInfo replica2Master1 = new NodeInfo("replica-2-master-1", "localhost", 7004, 7104);
        NodeInfo replica3Master1 = new NodeInfo("replica-3-master-1", "localhost", 7005, 7105);

        when(clusterManager.getAllNodes()).thenReturn(Set.of(master1));
        when(clusterManager.getReplicas(master1)).thenReturn(Set.of(replica1Master1, replica2Master1, replica3Master1));
        when(cacheClient.sendRequest(eq(master1), any(CacheRequest.class))).thenReturn(CompletableFuture.failedFuture(new RuntimeException("Connection refused")));

        CacheResponse statusResponse = new CacheResponse(
                "status-1",
                null,
                CacheResponse.Status.OK,
                null,
                "role:REPLICA,epoch:0"
        );

        CacheResponse promoteResponse = new CacheResponse(
                "promote-1",
                null,
                CacheResponse.Status.OK,
                null,
                "role:MASTER,epoch:123"
        );

        when(cacheClient.sendRequest(eq(replica1Master1), any(CacheRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(statusResponse))
                .thenReturn(CompletableFuture.completedFuture(promoteResponse));

        healthChecker.checkClusterHealth();
        healthChecker.checkClusterHealth();
        ArgumentCaptor<CacheRequest> requestCaptor = ArgumentCaptor.forClass(CacheRequest.class);
        verify(cacheClient, atLeast(2)).sendRequest(eq(replica1Master1), requestCaptor.capture());
        CacheRequest promoteRequest = requestCaptor.getAllValues().stream()
                .filter(req -> req.command() == CacheRequest.Command.PROMOTE)
                .findFirst()
                .orElse(null);

        assertNotNull(promoteRequest);
        assertTrue(promoteRequest.value().contains("epoch:"));
        assertTrue(promoteRequest.value().contains("replicas:"));
        assertTrue(promoteRequest.value().contains("replica-2-master-1"));
        assertTrue(promoteRequest.value().contains("replica-3-master-1"));
        assertFalse(promoteRequest.value().contains("replica-1-master-1"), "Should not include the promoted replica itself");
    }

    @Test
    @DisplayName("Should parse STATUS response with role and epoch")
    void shouldParseStatusResponseWithRoleAndEpoch() {
        when(clusterManager.getAllNodes()).thenReturn(Set.of(master1));

        CacheResponse statusResponse = new CacheResponse(
                "status-1",
                null,
                CacheResponse.Status.OK,
                null,
                "role:MASTER,epoch:42"
        );

        when(cacheClient.sendRequest(eq(master1), any(CacheRequest.class))).thenReturn(CompletableFuture.completedFuture(statusResponse));
        healthChecker.checkClusterHealth();
        verify(metrics, never()).recordHealthCheckFailure();
        assertEquals(0, healthChecker.getFailureCount("master-1"));
    }

    @Test
    @DisplayName("Should handle invalid STATUS response metadata gracefully")
    void shouldHandleInvalidStatusResponseMetadata() {
        when(clusterManager.getAllNodes()).thenReturn(Set.of(master1));

        CacheResponse invalidResponse = new CacheResponse(
                "status-1",
                null,
                CacheResponse.Status.OK,
                null,
                "invalid_format"
        );

        when(cacheClient.sendRequest(eq(master1), any(CacheRequest.class))).thenReturn(CompletableFuture.completedFuture(invalidResponse));
        healthChecker.checkClusterHealth();
        verify(metrics).recordHealthCheckFailure();
        assertEquals(1, healthChecker.getFailureCount("master-1"));
    }
}