package com.iksanov.distributedcache.node.net;

import com.iksanov.distributedcache.common.dto.CacheRequest;
import com.iksanov.distributedcache.common.dto.CacheResponse;
import com.iksanov.distributedcache.node.core.CacheStore;
import com.iksanov.distributedcache.node.metrics.NetMetrics;
import com.iksanov.distributedcache.node.replication.ReplicationManager;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import java.net.InetSocketAddress;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link NetConnectionHandler}.
 * <p>
 * Covers:
 *  - correct processing of GET/SET/DELETE via RequestProcessor (store mocked)
 *  - error handling when store throws CacheException or runtime errors
 *  - exceptionCaught() closes the context
 *  - EmbeddedChannel integration: real pipeline execution of channelRead0
 */
@ExtendWith(MockitoExtension.class)
class NetConnectionHandlerTest {

    @Mock
    private CacheStore store;
    @Mock
    private ReplicationManager replicationManager;
    @Mock
    private NetMetrics metrics;
    @Mock
    private ChannelHandlerContext ctx;
    @Mock
    private Channel channel;
    private NetConnectionHandler handler;
    private RequestProcessor processor;

    @BeforeEach
    void setUp() {
        processor = new RequestProcessor(store, replicationManager, metrics);
        handler = new NetConnectionHandler(processor, metrics);
    }

    @Test
    @DisplayName("Should call store.get for GET and write OK response")
    void shouldProcessGetAndWriteOkResponse() {
        CacheRequest req = mock(CacheRequest.class);
        when(req.requestId()).thenReturn("rid-1");
        when(req.command()).thenReturn(CacheRequest.Command.GET);
        when(req.key()).thenReturn("key-1");
        when(store.get("key-1")).thenReturn("value-1");
        ArgumentCaptor<CacheResponse> captor = ArgumentCaptor.forClass(CacheResponse.class);
        handler.channelRead0(ctx, req);
        verify(store).get("key-1");
        verify(ctx).writeAndFlush(captor.capture());
        CacheResponse resp = captor.getValue();
        assertNotNull(resp);
        assertEquals("rid-1", resp.requestId());
        assertEquals(CacheResponse.Status.OK, resp.status());
        assertEquals("value-1", resp.value());
    }

    @Test
    @DisplayName("Should return NOT_FOUND when store.get returns null")
    void getShouldReturnNotFoundWhenAbsent() {
        CacheRequest req = mock(CacheRequest.class);
        when(req.requestId()).thenReturn("rid-2");
        when(req.command()).thenReturn(CacheRequest.Command.GET);
        when(req.key()).thenReturn("missing");
        when(store.get("missing")).thenReturn(null);
        ArgumentCaptor<CacheResponse> captor = ArgumentCaptor.forClass(CacheResponse.class);
        handler.channelRead0(ctx, req);
        verify(store).get("missing");
        verify(ctx).writeAndFlush(captor.capture());
        CacheResponse resp = captor.getValue();
        assertEquals("rid-2", resp.requestId());
        assertEquals(CacheResponse.Status.NOT_FOUND, resp.status());
        assertNull(resp.value());
    }

    @Test
    @DisplayName("SET should call store.put and return OK")
    void setShouldPutAndReturnOk() {
        CacheRequest req = mock(CacheRequest.class);
        when(req.requestId()).thenReturn("rid-3");
        when(req.command()).thenReturn(CacheRequest.Command.SET);
        when(req.key()).thenReturn("k-set");
        when(req.value()).thenReturn("v-set");
        ArgumentCaptor<CacheResponse> captor = ArgumentCaptor.forClass(CacheResponse.class);
        handler.channelRead0(ctx, req);
        verify(store).put("k-set", "v-set");
        verify(ctx).writeAndFlush(captor.capture());
        CacheResponse resp = captor.getValue();
        assertEquals(CacheResponse.Status.OK, resp.status());
        assertEquals("rid-3", resp.requestId());
    }

    @Test
    @DisplayName("SET when store.put throws should return ERROR response")
    void setWhenStoreThrowsShouldReturnError() {
        CacheRequest req = mock(CacheRequest.class);
        when(req.requestId()).thenReturn("rid-4");
        when(req.command()).thenReturn(CacheRequest.Command.SET);
        when(req.key()).thenReturn("k-err");
        when(req.value()).thenReturn("v-err");
        doThrow(new RuntimeException("boom")).when(store).put("k-err", "v-err");
        ArgumentCaptor<CacheResponse> captor = ArgumentCaptor.forClass(CacheResponse.class);
        handler.channelRead0(ctx, req);
        verify(store).put("k-err", "v-err");
        verify(ctx).writeAndFlush(captor.capture());
        CacheResponse resp = captor.getValue();
        assertEquals(CacheResponse.Status.ERROR, resp.status());
        assertEquals("rid-4", resp.requestId());
        assertNotNull(resp.errorMessage());
    }

    @Test
    @DisplayName("DELETE should call store.delete and return OK")
    void deleteShouldDeleteAndReturnOk() {
        CacheRequest req = mock(CacheRequest.class);
        when(req.requestId()).thenReturn("rid-5");
        when(req.command()).thenReturn(CacheRequest.Command.DELETE);
        when(req.key()).thenReturn("k-del");
        ArgumentCaptor<CacheResponse> captor = ArgumentCaptor.forClass(CacheResponse.class);
        handler.channelRead0(ctx, req);
        verify(store).delete("k-del");
        verify(ctx).writeAndFlush(captor.capture());
        CacheResponse resp = captor.getValue();
        assertEquals(CacheResponse.Status.OK, resp.status());
    }

    @Test
    @DisplayName("exceptionCaught should call ctx.close()")
    void shouldCloseChannelOnExceptionCaught() {
        Throwable cause = new RuntimeException("test-ex");
        when(ctx.channel()).thenReturn(channel);
        when(channel.remoteAddress()).thenReturn(new InetSocketAddress("127.0.0.1", 9000));
        handler.exceptionCaught(ctx, cause);
        verify(ctx).close();
    }

    @Test
    @DisplayName("Integration: EmbeddedChannel should accept inbound request and produce outbound response")
    void embeddedChannelIntegrationTest() {
        when(store.get("k-emb")).thenReturn("v-emb");
        EmbeddedChannel embedded = new EmbeddedChannel(new NetConnectionHandler(processor, metrics));
        CacheRequest req = mock(CacheRequest.class);
        when(req.requestId()).thenReturn("rid-emb");
        when(req.command()).thenReturn(CacheRequest.Command.GET);
        when(req.key()).thenReturn("k-emb");
        embedded.writeInbound(req);
        Object outbound = embedded.readOutbound();
        assertNotNull(outbound, "Expected outbound CacheResponse from handler");
        assertInstanceOf(CacheResponse.class, outbound);
        CacheResponse resp = (CacheResponse) outbound;
        assertEquals(CacheResponse.Status.OK, resp.status());
        assertEquals("v-emb", resp.value());
        embedded.finishAndReleaseAll();
    }
}
