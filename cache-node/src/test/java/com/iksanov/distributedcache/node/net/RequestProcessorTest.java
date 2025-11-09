package com.iksanov.distributedcache.node.net;

import com.iksanov.distributedcache.common.dto.CacheRequest;
import com.iksanov.distributedcache.common.dto.CacheResponse;
import com.iksanov.distributedcache.common.exception.CacheException;
import com.iksanov.distributedcache.node.core.CacheStore;
import com.iksanov.distributedcache.node.metrics.NetMetrics;
import com.iksanov.distributedcache.node.replication.ReplicationManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link RequestProcessor}.
 * <p>
 * Covers:
 *  - GET/SET/DELETE logic
 *  - key/value validation
 *  - replication trigger behavior
 *  - exception handling
 */
@ExtendWith(MockitoExtension.class)
public class RequestProcessorTest {

    @Mock
    CacheStore store;
    @Mock
    ReplicationManager replicationManager;
    @Mock
    NetMetrics netMetrics;
    RequestProcessor processor;
    @Captor
    ArgumentCaptor<String> keyCaptor;
    @Captor
    ArgumentCaptor<String> valueCaptor;

    @BeforeEach
    void setUp() {
        processor = new RequestProcessor(store, replicationManager, netMetrics);
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
}
