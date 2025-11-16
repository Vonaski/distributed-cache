package com.iksanov.distributedcache.node.net;

import com.iksanov.distributedcache.common.dto.CacheRequest;
import com.iksanov.distributedcache.common.dto.CacheResponse;
import com.iksanov.distributedcache.node.config.NetServerConfig;
import com.iksanov.distributedcache.node.consensus.model.Command;
import com.iksanov.distributedcache.node.consensus.raft.RaftNode;
import com.iksanov.distributedcache.node.consensus.raft.RaftStateMachine;
import com.iksanov.distributedcache.node.consensus.sharding.ShardManager;
import com.iksanov.distributedcache.node.metrics.NetMetrics;
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
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link NetConnectionHandler}.
 * <p>
 * Covers:
 *  - correct processing of GET/SET/DELETE via RequestProcessor (with ShardManager mocked)
 *  - error handling when consensus fails or components unavailable
 *  - exceptionCaught() closes the context
 *  - EmbeddedChannel integration: real pipeline execution of channelRead0
 */
@ExtendWith(MockitoExtension.class)
class NetConnectionHandlerTest {

    private static final String SHARD_ID = "shard-0";

    @Mock
    private ShardManager shardManager;
    @Mock
    private RaftNode raftNode;
    @Mock
    private RaftStateMachine stateMachine;
    @Mock
    private NetMetrics metrics;
    @Mock
    private ChannelHandlerContext ctx;
    @Mock
    private Channel channel;

    private NetConnectionHandler handler;
    private RequestProcessor processor;
    private NetServerConfig config;

    @BeforeEach
    void setUp() {
        config = new NetServerConfig(
                "localhost", 7000, 1, 1, 128,
                16 * 1024 * 1024, 2, 10, 100L, 10L
        );
        processor = new RequestProcessor(shardManager, metrics, config);
        handler = new NetConnectionHandler(processor, metrics);

        // Default mocks behavior
        lenient().when(shardManager.selectShardForKey(any())).thenReturn(SHARD_ID);
        lenient().when(shardManager.getShard(SHARD_ID)).thenReturn(raftNode);
        lenient().when(shardManager.getStateMachine(SHARD_ID)).thenReturn(stateMachine);

        // Setup metrics mocks with lenient() to avoid UnnecessaryStubbingException
        lenient().doNothing().when(metrics).incrementRequests();
        lenient().doNothing().when(metrics).incrementResponses();
        lenient().doNothing().when(metrics).incrementErrors();
        lenient().doNothing().when(metrics).recordRequestDuration(anyLong());
    }

    @Test
    @DisplayName("Should process GET and write OK response when value exists")
    void shouldProcessGetAndWriteOkResponse() {
        CacheRequest req = mock(CacheRequest.class);
        when(req.requestId()).thenReturn("rid-1");
        when(req.command()).thenReturn(CacheRequest.Command.GET);
        when(req.key()).thenReturn("key-1");
        when(stateMachine.get("key-1")).thenReturn("value-1");

        ArgumentCaptor<CacheResponse> captor = ArgumentCaptor.forClass(CacheResponse.class);
        handler.channelRead0(ctx, req);

        verify(stateMachine).get("key-1");
        verify(ctx).writeAndFlush(captor.capture());
        CacheResponse resp = captor.getValue();
        assertNotNull(resp);
        assertEquals("rid-1", resp.requestId());
        assertEquals(CacheResponse.Status.OK, resp.status());
        assertEquals("value-1", resp.value());
    }

    @Test
    @DisplayName("Should return NOT_FOUND when key doesn't exist")
    void getShouldReturnNotFoundWhenAbsent() {
        CacheRequest req = mock(CacheRequest.class);
        when(req.requestId()).thenReturn("rid-2");
        when(req.command()).thenReturn(CacheRequest.Command.GET);
        when(req.key()).thenReturn("missing");
        when(stateMachine.get("missing")).thenReturn(null);

        ArgumentCaptor<CacheResponse> captor = ArgumentCaptor.forClass(CacheResponse.class);
        handler.channelRead0(ctx, req);

        verify(stateMachine).get("missing");
        verify(ctx).writeAndFlush(captor.capture());
        CacheResponse resp = captor.getValue();
        assertEquals("rid-2", resp.requestId());
        assertEquals(CacheResponse.Status.NOT_FOUND, resp.status());
        assertNull(resp.value());
    }

    @Test
    @DisplayName("SET should submit to Raft and return OK")
    void setShouldSubmitToRaftAndReturnOk() {
        CacheRequest req = mock(CacheRequest.class);
        when(req.requestId()).thenReturn("rid-3");
        when(req.command()).thenReturn(CacheRequest.Command.SET);
        when(req.key()).thenReturn("k-set");
        when(req.value()).thenReturn("v-set");
        when(raftNode.submit(any(Command.class))).thenReturn(CompletableFuture.completedFuture(1L));

        ArgumentCaptor<CacheResponse> captor = ArgumentCaptor.forClass(CacheResponse.class);
        handler.channelRead0(ctx, req);

        verify(raftNode).submit(any(Command.class));
        verify(ctx).writeAndFlush(captor.capture());
        CacheResponse resp = captor.getValue();
        assertEquals(CacheResponse.Status.OK, resp.status());
        assertEquals("rid-3", resp.requestId());
    }

    @Test
    @DisplayName("SET when consensus fails should return ERROR response")
    void setWhenConsensusFailsShouldReturnError() {
        CacheRequest req = mock(CacheRequest.class);
        when(req.requestId()).thenReturn("rid-4");
        when(req.command()).thenReturn(CacheRequest.Command.SET);
        when(req.key()).thenReturn("k-err");
        when(req.value()).thenReturn("v-err");

        CompletableFuture<Long> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(new RuntimeException("Consensus failed"));
        when(raftNode.submit(any(Command.class))).thenReturn(failedFuture);

        ArgumentCaptor<CacheResponse> captor = ArgumentCaptor.forClass(CacheResponse.class);
        handler.channelRead0(ctx, req);

        verify(raftNode).submit(any(Command.class));
        verify(ctx).writeAndFlush(captor.capture());
        CacheResponse resp = captor.getValue();
        assertEquals(CacheResponse.Status.ERROR, resp.status());
        assertEquals("rid-4", resp.requestId());
        assertNotNull(resp.errorMessage());
        assertTrue(resp.errorMessage().contains("Consensus failed"));
    }

    @Test
    @DisplayName("DELETE should submit to Raft and return OK")
    void deleteShouldSubmitToRaftAndReturnOk() {
        CacheRequest req = mock(CacheRequest.class);
        when(req.requestId()).thenReturn("rid-5");
        when(req.command()).thenReturn(CacheRequest.Command.DELETE);
        when(req.key()).thenReturn("k-del");
        when(raftNode.submit(any(Command.class))).thenReturn(CompletableFuture.completedFuture(2L));

        ArgumentCaptor<CacheResponse> captor = ArgumentCaptor.forClass(CacheResponse.class);
        handler.channelRead0(ctx, req);

        verify(raftNode).submit(any(Command.class));
        verify(ctx).writeAndFlush(captor.capture());
        CacheResponse resp = captor.getValue();
        assertEquals(CacheResponse.Status.OK, resp.status());
    }

    @Test
    @DisplayName("GET should return ERROR when shard not found")
    void getShouldReturnErrorWhenShardNotFound() {
        when(shardManager.selectShardForKey(any())).thenReturn(null);

        CacheRequest req = mock(CacheRequest.class);
        when(req.requestId()).thenReturn("rid-6");
        when(req.command()).thenReturn(CacheRequest.Command.GET);
        when(req.key()).thenReturn("key-no-shard");

        ArgumentCaptor<CacheResponse> captor = ArgumentCaptor.forClass(CacheResponse.class);
        handler.channelRead0(ctx, req);

        verify(ctx).writeAndFlush(captor.capture());
        CacheResponse resp = captor.getValue();
        assertEquals(CacheResponse.Status.ERROR, resp.status());
        assertTrue(resp.errorMessage().contains("No shard available"));
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
        when(stateMachine.get("k-emb")).thenReturn("v-emb");

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