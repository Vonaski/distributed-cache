package com.iksanov.distributedcache.node.net;

import com.iksanov.distributedcache.common.cluster.NodeInfo;
import com.iksanov.distributedcache.common.dto.CacheRequest;
import com.iksanov.distributedcache.common.dto.CacheResponse;
import com.iksanov.distributedcache.common.exception.CacheException;
import com.iksanov.distributedcache.node.config.ApplicationConfig.NodeRole;
import com.iksanov.distributedcache.node.core.CacheStore;
import com.iksanov.distributedcache.node.metrics.NetMetrics;
import com.iksanov.distributedcache.node.replication.ReplicationManager;
import com.iksanov.distributedcache.node.replication.failover.FailoverManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link RequestProcessor}.
 * <p>
 * Covers:
 *  - GET/SET/DELETE logic
 *  - key/value validation
 *  - replication trigger behavior
 *  - role-based access control
 *  - exception handling
 *  - STATUS and PROMOTE admin commands
 */
@ExtendWith(MockitoExtension.class)
public class RequestProcessorTest {

    @Mock
    CacheStore store;
    @Mock
    ReplicationManager replicationManager;
    @Mock
    NetMetrics netMetrics;
    @Mock
    FailoverManager failoverManager;

    RequestProcessor processor;
    RequestProcessor replicaProcessor;
    RequestProcessor processorWithFailover;

    @Captor
    ArgumentCaptor<String> keyCaptor;
    @Captor
    ArgumentCaptor<String> valueCaptor;

    @BeforeEach
    void setUp() {
        processor = new RequestProcessor(store, replicationManager, NodeRole.MASTER, netMetrics, null);
        replicaProcessor = new RequestProcessor(store, replicationManager, NodeRole.REPLICA, netMetrics, null);
        processorWithFailover = new RequestProcessor(store, replicationManager, NodeRole.REPLICA, netMetrics, failoverManager);
    }

    @Test
    @DisplayName("SET should store value and trigger replication")
    void setShouldPutAndReplicate() {
        CacheRequest req = mock(CacheRequest.class);
        when(req.command()).thenReturn(CacheRequest.Command.SET);
        when(req.key()).thenReturn("k1");
        when(req.value()).thenReturn("v1");
        when(req.requestId()).thenReturn("rid-1");
        CacheResponse resp = processor.process(req);
        assertNotNull(resp);
        verify(store, times(1)).put(keyCaptor.capture(), valueCaptor.capture());
        assertEquals("k1", keyCaptor.getValue());
        assertEquals("v1", valueCaptor.getValue());
        verify(replicationManager, times(1)).onLocalSet("k1", "v1");
    }

    @Test
    @DisplayName("DELETE should remove value and trigger replication")
    void deleteShouldDeleteAndReplicate() {
        CacheRequest req = mock(CacheRequest.class);
        when(req.command()).thenReturn(CacheRequest.Command.DELETE);
        when(req.key()).thenReturn("k-delete");
        when(req.requestId()).thenReturn("rid-2");
        CacheResponse resp = processor.process(req);
        assertNotNull(resp);
        verify(store, times(1)).delete(keyCaptor.capture());
        assertEquals("k-delete", keyCaptor.getValue());
        verify(replicationManager, times(1)).onLocalDelete("k-delete");
    }

    @Test
    @DisplayName("GET should return found value and not call replication")
    void getShouldReturnWhenFound() {
        CacheRequest req = mock(CacheRequest.class);
        when(req.command()).thenReturn(CacheRequest.Command.GET);
        when(req.key()).thenReturn("k-get");
        when(req.requestId()).thenReturn("rid-3");
        when(store.get("k-get")).thenReturn("v-123");
        CacheResponse resp = processor.process(req);
        assertNotNull(resp);
        verify(store, times(1)).get("k-get");
        verifyNoInteractions(replicationManager);
    }

    @Test
    @DisplayName("GET should return notFound when store returns null")
    void getShouldReturnNotFoundWhenAbsent() {
        CacheRequest req = mock(CacheRequest.class);
        when(req.command()).thenReturn(CacheRequest.Command.GET);
        when(req.key()).thenReturn("k-miss");
        when(req.requestId()).thenReturn("rid-4");
        when(store.get("k-miss")).thenReturn(null);
        CacheResponse resp = processor.process(req);
        assertNotNull(resp);
        verify(store, times(1)).get("k-miss");
        verifyNoInteractions(replicationManager);
    }

    @Test
    @DisplayName("SET when store throws should return error and not replicate")
    void setWhenStoreThrowsShouldReturnErrorAndNotReplicate() {
        CacheRequest req = mock(CacheRequest.class);
        when(req.command()).thenReturn(CacheRequest.Command.SET);
        when(req.key()).thenReturn("k-ex");
        when(req.value()).thenReturn("v-ex");
        when(req.requestId()).thenReturn("rid-5");
        doThrow(new CacheException("boom")).when(store).put("k-ex", "v-ex");
        CacheResponse resp = processor.process(req);
        assertNotNull(resp);
        verify(store, times(1)).put("k-ex", "v-ex");
        verify(replicationManager, never()).onLocalSet(anyString(), anyString());
    }

    @Test
    @DisplayName("SET with null key should return error and not call store/replication")
    void invalidRequestNullKeyShouldReturnError() {
        CacheRequest req = mock(CacheRequest.class);
        when(req.command()).thenReturn(CacheRequest.Command.SET);
        when(req.key()).thenReturn(null);
        when(req.requestId()).thenReturn("rid-6");
        CacheResponse resp = processor.process(req);
        assertNotNull(resp);
        verifyNoInteractions(store);
        verifyNoInteractions(replicationManager);
    }

    @Test
    @DisplayName("DELETE with blank key should return error and not call store/replication")
    void invalidRequestBlankKeyShouldReturnError() {
        CacheRequest req = mock(CacheRequest.class);
        when(req.command()).thenReturn(CacheRequest.Command.DELETE);
        when(req.key()).thenReturn("   ");
        when(req.requestId()).thenReturn("rid-7");
        CacheResponse resp = processor.process(req);
        assertNotNull(resp);
        verifyNoInteractions(store);
        verifyNoInteractions(replicationManager);
    }

    @Test
    @DisplayName("SET with null value should return error and not call store/replication")
    void setWithNullValueShouldReturnError() {
        CacheRequest req = mock(CacheRequest.class);
        when(req.command()).thenReturn(CacheRequest.Command.SET);
        when(req.key()).thenReturn("key-null-value");
        when(req.value()).thenReturn(null);
        when(req.requestId()).thenReturn("rid-8");
        CacheResponse resp = processor.process(req);
        assertNotNull(resp);
        verifyNoInteractions(store);
        verifyNoInteractions(replicationManager);
    }

    @Test
    @DisplayName("REPLICA should reject SET requests")
    void replicaShouldRejectSet() {
        CacheRequest req = mock(CacheRequest.class);
        when(req.command()).thenReturn(CacheRequest.Command.SET);
        when(req.requestId()).thenReturn("rid-9");
        CacheResponse resp = replicaProcessor.process(req);
        assertNotNull(resp);
        assertEquals(CacheResponse.Status.ERROR, resp.status());
        assertTrue(resp.errorMessage().contains("Cannot write to replica"));
        verifyNoInteractions(store);
        verifyNoInteractions(replicationManager);
    }

    @Test
    @DisplayName("REPLICA should reject DELETE requests")
    void replicaShouldRejectDelete() {
        CacheRequest req = mock(CacheRequest.class);
        when(req.command()).thenReturn(CacheRequest.Command.DELETE);
        when(req.requestId()).thenReturn("rid-10");
        CacheResponse resp = replicaProcessor.process(req);
        assertNotNull(resp);
        assertEquals(CacheResponse.Status.ERROR, resp.status());
        assertTrue(resp.errorMessage().contains("Cannot write to replica"));
        verifyNoInteractions(store);
        verifyNoInteractions(replicationManager);
    }

    @Test
    @DisplayName("REPLICA should allow GET requests")
    void replicaShouldAllowGet() {
        CacheRequest req = mock(CacheRequest.class);
        when(req.command()).thenReturn(CacheRequest.Command.GET);
        when(req.key()).thenReturn("k-get");
        when(req.requestId()).thenReturn("rid-11");
        when(store.get("k-get")).thenReturn("v-123");
        CacheResponse resp = replicaProcessor.process(req);
        assertNotNull(resp);
        assertEquals(CacheResponse.Status.OK, resp.status());
        verify(store, times(1)).get("k-get");
        verifyNoInteractions(replicationManager);
    }

    @Test
    @DisplayName("STATUS should return role and epoch when failoverManager is present")
    void statusShouldReturnRoleAndEpochWithFailover() {
        when(failoverManager.getCurrentRole()).thenReturn(NodeRole.REPLICA);
        when(failoverManager.getEpoch()).thenReturn(42L);
        CacheRequest req = mock(CacheRequest.class);
        when(req.command()).thenReturn(CacheRequest.Command.STATUS);
        when(req.requestId()).thenReturn("status-1");
        CacheResponse resp = processorWithFailover.process(req);
        assertNotNull(resp);
        assertEquals(CacheResponse.Status.OK, resp.status());
        assertNotNull(resp.metadata());
        assertEquals("role:REPLICA,epoch:42", resp.metadata());
        verifyNoInteractions(store);
        verifyNoInteractions(replicationManager);
    }

    @Test
    @DisplayName("STATUS should return role with epoch 0 when failoverManager is null")
    void statusShouldReturnRoleWithEpoch0WhenNoFailover() {
        CacheRequest req = mock(CacheRequest.class);
        when(req.command()).thenReturn(CacheRequest.Command.STATUS);
        when(req.requestId()).thenReturn("status-2");
        CacheResponse resp = processor.process(req);
        assertNotNull(resp);
        assertEquals(CacheResponse.Status.OK, resp.status());
        assertNotNull(resp.metadata());
        assertEquals("role:MASTER,epoch:0", resp.metadata());
        verifyNoInteractions(store);
        verifyNoInteractions(replicationManager);
    }

    @Test
    @DisplayName("STATUS should work for MASTER role")
    void statusShouldWorkForMaster() {
        when(failoverManager.getCurrentRole()).thenReturn(NodeRole.MASTER);
        when(failoverManager.getEpoch()).thenReturn(999L);
        CacheRequest req = mock(CacheRequest.class);
        when(req.command()).thenReturn(CacheRequest.Command.STATUS);
        when(req.requestId()).thenReturn("status-3");
        CacheResponse resp = processorWithFailover.process(req);
        assertNotNull(resp);
        assertEquals(CacheResponse.Status.OK, resp.status());
        assertEquals("role:MASTER,epoch:999", resp.metadata());
    }

    @Test
    @DisplayName("PROMOTE should fail when failoverManager is null (standalone mode)")
    void promoteShouldFailWhenNoFailoverManager() {
        CacheRequest req = mock(CacheRequest.class);
        when(req.command()).thenReturn(CacheRequest.Command.PROMOTE);
        when(req.requestId()).thenReturn("promote-1");
        CacheResponse resp = processor.process(req);
        assertNotNull(resp);
        assertEquals(CacheResponse.Status.ERROR, resp.status());
        assertTrue(resp.errorMessage().contains("standalone mode"));
        verifyNoInteractions(store);
        verifyNoInteractions(replicationManager);
    }

    @Test
    @DisplayName("PROMOTE should fail when epoch format is invalid")
    void promoteShouldFailWhenEpochFormatInvalid() {
        CacheRequest req = mock(CacheRequest.class);
        when(req.command()).thenReturn(CacheRequest.Command.PROMOTE);
        when(req.requestId()).thenReturn("promote-2");
        when(req.value()).thenReturn("invalid-format");
        CacheResponse resp = processorWithFailover.process(req);
        assertNotNull(resp);
        assertEquals(CacheResponse.Status.ERROR, resp.status());
        assertTrue(resp.errorMessage().contains("Invalid PROMOTE request format"));
        verifyNoInteractions(failoverManager);
    }

    @Test
    @DisplayName("PROMOTE should fail when value is null")
    void promoteShouldFailWhenValueIsNull() {
        CacheRequest req = mock(CacheRequest.class);
        when(req.command()).thenReturn(CacheRequest.Command.PROMOTE);
        when(req.requestId()).thenReturn("promote-3");
        when(req.value()).thenReturn(null);
        CacheResponse resp = processorWithFailover.process(req);
        assertNotNull(resp);
        assertEquals(CacheResponse.Status.ERROR, resp.status());
        assertTrue(resp.errorMessage().contains("Invalid PROMOTE request format"));
        verifyNoInteractions(failoverManager);
    }

    @Test
    @DisplayName("PROMOTE should fail when epoch is not a number")
    void promoteShouldFailWhenEpochNotNumber() {
        CacheRequest req = mock(CacheRequest.class);
        when(req.command()).thenReturn(CacheRequest.Command.PROMOTE);
        when(req.requestId()).thenReturn("promote-4");
        when(req.value()).thenReturn("epoch:notanumber");
        CacheResponse resp = processorWithFailover.process(req);
        assertNotNull(resp);
        assertEquals(CacheResponse.Status.ERROR, resp.status());
        assertTrue(resp.errorMessage().contains("Invalid epoch format"));
        verifyNoInteractions(failoverManager);
    }

    @Test
    @DisplayName("PROMOTE should succeed with valid epoch only (no replicas)")
    void promoteShouldSucceedWithEpochOnly() {
        when(failoverManager.getEpoch()).thenReturn(100L);
        when(failoverManager.getCurrentRole()).thenReturn(NodeRole.MASTER);
        CacheRequest req = mock(CacheRequest.class);
        when(req.command()).thenReturn(CacheRequest.Command.PROMOTE);
        when(req.requestId()).thenReturn("promote-5");
        when(req.value()).thenReturn("epoch:200");
        CacheResponse resp = processorWithFailover.process(req);
        assertNotNull(resp);
        assertEquals(CacheResponse.Status.OK, resp.status());
        assertNotNull(resp.metadata());
        assertTrue(resp.metadata().contains("role:MASTER"));
        ArgumentCaptor<Long> epochCaptor = ArgumentCaptor.forClass(Long.class);
        ArgumentCaptor<List> replicasCaptor = ArgumentCaptor.forClass(List.class);
        verify(failoverManager).promoteToMasterWithEpoch(epochCaptor.capture(), replicasCaptor.capture());
        assertEquals(200L, epochCaptor.getValue());
        assertTrue(replicasCaptor.getValue().isEmpty());
    }

    @Test
    @DisplayName("PROMOTE should succeed with epoch and replica list")
    void promoteShouldSucceedWithEpochAndReplicas() {
        when(failoverManager.getEpoch()).thenReturn(100L);
        when(failoverManager.getCurrentRole()).thenReturn(NodeRole.MASTER);
        CacheRequest req = mock(CacheRequest.class);
        when(req.command()).thenReturn(CacheRequest.Command.PROMOTE);
        when(req.requestId()).thenReturn("promote-6");
        when(req.value()).thenReturn("epoch:300,replicas:replica-2:localhost:7004:7104;replica-3:localhost:7005:7105");
        CacheResponse resp = processorWithFailover.process(req);
        assertNotNull(resp);
        assertEquals(CacheResponse.Status.OK, resp.status());
        ArgumentCaptor<Long> epochCaptor = ArgumentCaptor.forClass(Long.class);
        ArgumentCaptor<List> replicasCaptor = ArgumentCaptor.forClass(List.class);
        verify(failoverManager).promoteToMasterWithEpoch(epochCaptor.capture(), replicasCaptor.capture());
        assertEquals(300L, epochCaptor.getValue());
        assertEquals(2, replicasCaptor.getValue().size());
        List<NodeInfo> replicas = replicasCaptor.getValue();
        NodeInfo replica2 = replicas.getFirst();
        assertEquals("replica-2", replica2.nodeId());
        assertEquals("localhost", replica2.host());
        assertEquals(7004, replica2.port());
        assertEquals(7104, replica2.replicationPort());
        NodeInfo replica3 = replicas.get(1);
        assertEquals("replica-3", replica3.nodeId());
        assertEquals(7005, replica3.port());
    }

    @Test
    @DisplayName("PROMOTE should ignore already promoted node with same epoch")
    void promoteShouldIgnoreAlreadyPromotedWithSameEpoch() {
        when(failoverManager.getCurrentRole()).thenReturn(NodeRole.MASTER);
        when(failoverManager.getEpoch()).thenReturn(500L);
        CacheRequest req = mock(CacheRequest.class);
        when(req.command()).thenReturn(CacheRequest.Command.PROMOTE);
        when(req.requestId()).thenReturn("promote-7");
        when(req.value()).thenReturn("epoch:500");
        CacheResponse resp = processorWithFailover.process(req);
        assertNotNull(resp);
        assertEquals(CacheResponse.Status.OK, resp.status());
        assertTrue(resp.metadata().contains("epoch:500"));
        verify(failoverManager, never()).promoteToMasterWithEpoch(anyLong(), any());
    }

    @Test
    @DisplayName("PROMOTE should fail when requested epoch is lower than current")
    void promoteShouldFailWhenEpochTooOld() {
        when(failoverManager.getCurrentRole()).thenReturn(NodeRole.REPLICA);
        when(failoverManager.getEpoch()).thenReturn(500L);
        CacheRequest req = mock(CacheRequest.class);
        when(req.command()).thenReturn(CacheRequest.Command.PROMOTE);
        when(req.requestId()).thenReturn("promote-8");
        when(req.value()).thenReturn("epoch:400");
        CacheResponse resp = processorWithFailover.process(req);
        assertNotNull(resp);
        assertEquals(CacheResponse.Status.ERROR, resp.status());
        assertTrue(resp.errorMessage().contains("Epoch too old"));
        assertTrue(resp.errorMessage().contains("500"));
        assertTrue(resp.errorMessage().contains("400"));
        verify(failoverManager, never()).promoteToMasterWithEpoch(anyLong(), any());
    }

    @Test
    @DisplayName("PROMOTE should gracefully handle invalid replica format in list")
    void promoteShouldHandleInvalidReplicaFormat() {
        when(failoverManager.getEpoch()).thenReturn(100L);
        when(failoverManager.getCurrentRole()).thenReturn(NodeRole.MASTER);
        CacheRequest req = mock(CacheRequest.class);
        when(req.command()).thenReturn(CacheRequest.Command.PROMOTE);
        when(req.requestId()).thenReturn("promote-9");
        when(req.value()).thenReturn("epoch:350,replicas:invalid-format;replica-2:localhost:7004:7104");
        CacheResponse resp = processorWithFailover.process(req);
        assertNotNull(resp);
        assertEquals(CacheResponse.Status.OK, resp.status());
        ArgumentCaptor<List> replicasCaptor = ArgumentCaptor.forClass(List.class);
        verify(failoverManager).promoteToMasterWithEpoch(eq(350L), replicasCaptor.capture());
        List<NodeInfo> replicas = replicasCaptor.getValue();
        assertEquals(1, replicas.size()); // Only valid replica parsed
        assertEquals("replica-2", replicas.getFirst().nodeId());
    }

    @Test
    @DisplayName("PROMOTE should handle empty replicas list")
    void promoteShouldHandleEmptyReplicasList() {
        when(failoverManager.getEpoch()).thenReturn(100L);
        when(failoverManager.getCurrentRole()).thenReturn(NodeRole.MASTER);
        CacheRequest req = mock(CacheRequest.class);
        when(req.command()).thenReturn(CacheRequest.Command.PROMOTE);
        when(req.requestId()).thenReturn("promote-10");
        when(req.value()).thenReturn("epoch:400,replicas:");
        CacheResponse resp = processorWithFailover.process(req);
        assertNotNull(resp);
        assertEquals(CacheResponse.Status.OK, resp.status());
        ArgumentCaptor<List> replicasCaptor = ArgumentCaptor.forClass(List.class);
        verify(failoverManager).promoteToMasterWithEpoch(eq(400L), replicasCaptor.capture());
        assertTrue(replicasCaptor.getValue().isEmpty());
    }

    @Test
    @DisplayName("updateRole should change role correctly")
    void updateRoleShouldChangeRole() {
        assertEquals(NodeRole.MASTER, processor.getCurrentRole());
        processor.updateRole(NodeRole.REPLICA);
        assertEquals(NodeRole.REPLICA, processor.getCurrentRole());
    }

    @Test
    @DisplayName("After role update to MASTER, SET should work")
    void afterRoleUpdateToMasterSetShouldWork() {
        assertEquals(NodeRole.REPLICA, replicaProcessor.getCurrentRole());
        replicaProcessor.updateRole(NodeRole.MASTER);
        CacheRequest req = mock(CacheRequest.class);
        when(req.command()).thenReturn(CacheRequest.Command.SET);
        when(req.key()).thenReturn("key-after-promote");
        when(req.value()).thenReturn("value-after-promote");
        when(req.requestId()).thenReturn("req-promote");
        CacheResponse resp = replicaProcessor.process(req);
        assertNotNull(resp);
        assertEquals(CacheResponse.Status.OK, resp.status());
        verify(store).put("key-after-promote", "value-after-promote");
        verify(replicationManager).onLocalSet("key-after-promote", "value-after-promote");
    }
}
