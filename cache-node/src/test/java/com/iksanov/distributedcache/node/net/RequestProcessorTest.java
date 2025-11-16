package com.iksanov.distributedcache.node.net;

import com.iksanov.distributedcache.common.dto.CacheRequest;
import com.iksanov.distributedcache.common.dto.CacheResponse;
import com.iksanov.distributedcache.node.config.NetServerConfig;
import com.iksanov.distributedcache.node.consensus.model.Command;
import com.iksanov.distributedcache.node.consensus.raft.RaftNode;
import com.iksanov.distributedcache.node.consensus.raft.RaftStateMachine;
import com.iksanov.distributedcache.node.consensus.sharding.ShardManager;
import com.iksanov.distributedcache.node.metrics.NetMetrics;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link RequestProcessor}.
 * <p>
 * Covers:
 *  - GET/SET/DELETE logic with Raft consensus
 *  - key/value validation
 *  - ShardManager interaction
 *  - exception handling
 */
@ExtendWith(MockitoExtension.class)
public class RequestProcessorTest {

    private static final String SHARD_ID = "shard-0";

    @Mock
    ShardManager shardManager;
    @Mock
    RaftNode raftNode;
    @Mock
    RaftStateMachine stateMachine;
    @Mock
    NetMetrics netMetrics;

    NetServerConfig config;
    RequestProcessor processor;

    @Captor
    ArgumentCaptor<Command> commandCaptor;

    @BeforeEach
    void setUp() {
        config = new NetServerConfig(
                "localhost", 7000, 1, 1, 128,
                16 * 1024 * 1024, 2, 10, 100L, 10L
        );
        processor = new RequestProcessor(shardManager, netMetrics, config);

        lenient().when(shardManager.selectShardForKey(any())).thenReturn(SHARD_ID);
        lenient().when(shardManager.getShard(SHARD_ID)).thenReturn(raftNode);
        lenient().when(shardManager.getStateMachine(SHARD_ID)).thenReturn(stateMachine);
        lenient().doNothing().when(netMetrics).incrementRequests();
        lenient().doNothing().when(netMetrics).incrementResponses();
        lenient().doNothing().when(netMetrics).incrementErrors();
        lenient().doNothing().when(netMetrics).recordRequestDuration(anyLong());
    }

    @Test
    @DisplayName("GET should read from StateMachine and return OK when value exists")
    void getShouldReturnValueFromStateMachine() {
        CacheRequest req = createRequest(CacheRequest.Command.GET, "key1", null);
        when(stateMachine.get("key1")).thenReturn("value1");
        CacheResponse resp = processor.process(req);
        assertNotNull(resp);
        assertEquals(CacheResponse.Status.OK, resp.status());
        assertEquals("value1", resp.value());
        verify(stateMachine, times(1)).get("key1");
        verifyNoInteractions(raftNode);
    }

    @Test
    @DisplayName("GET should return NOT_FOUND when key doesn't exist")
    void getShouldReturnNotFoundWhenKeyAbsent() {
        CacheRequest req = createRequest(CacheRequest.Command.GET, "missing-key", null);
        when(stateMachine.get("missing-key")).thenReturn(null);
        CacheResponse resp = processor.process(req);
        assertNotNull(resp);
        assertEquals(CacheResponse.Status.NOT_FOUND, resp.status());
        verify(stateMachine, times(1)).get("missing-key");
    }

    @Test
    @DisplayName("SET should submit command to Raft and return OK on success")
    void setShouldSubmitToRaftAndReturnOk() {
        CacheRequest req = createRequest(CacheRequest.Command.SET, "key2", "value2");
        when(raftNode.submit(any(Command.class))).thenReturn(CompletableFuture.completedFuture(1L));
        CacheResponse resp = processor.process(req);
        assertNotNull(resp);
        assertEquals(CacheResponse.Status.OK, resp.status());
        verify(raftNode, times(1)).submit(commandCaptor.capture());
        Command capturedCmd = commandCaptor.getValue();
        assertEquals(Command.Type.SET, capturedCmd.type());
        assertEquals("key2", capturedCmd.key());
        assertEquals("value2", capturedCmd.value());
    }

    @Test
    @DisplayName("DELETE should submit command to Raft and return OK on success")
    void deleteShouldSubmitToRaftAndReturnOk() {
        CacheRequest req = createRequest(CacheRequest.Command.DELETE, "key-delete", null);
        when(raftNode.submit(any(Command.class))).thenReturn(CompletableFuture.completedFuture(2L));
        CacheResponse resp = processor.process(req);
        assertNotNull(resp);
        assertEquals(CacheResponse.Status.OK, resp.status());
        verify(raftNode, times(1)).submit(commandCaptor.capture());
        Command capturedCmd = commandCaptor.getValue();
        assertEquals(Command.Type.DELETE, capturedCmd.type());
        assertEquals("key-delete", capturedCmd.key());
    }

    @Test
    @DisplayName("SET should return error when Raft consensus fails")
    void setShouldReturnErrorWhenConsensusFails() {
        CacheRequest req = createRequest(CacheRequest.Command.SET, "key-fail", "value-fail");
        CompletableFuture<Long> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(new TimeoutException("Consensus timeout"));
        when(raftNode.submit(any(Command.class))).thenReturn(failedFuture);
        CacheResponse resp = processor.process(req);
        assertNotNull(resp);
        assertEquals(CacheResponse.Status.ERROR, resp.status());
        assertNotNull(resp.errorMessage());
        assertTrue(resp.errorMessage().contains("Consensus failed"));
    }

    @Test
    @DisplayName("SET should return error when not leader")
    void setShouldReturnErrorWhenNotLeader() {
        CacheRequest req = createRequest(CacheRequest.Command.SET, "key-not-leader", "value");
        CompletableFuture<Long> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(new IllegalStateException("Not leader"));
        when(raftNode.submit(any(Command.class))).thenReturn(failedFuture);
        CacheResponse resp = processor.process(req);
        assertNotNull(resp);
        assertEquals(CacheResponse.Status.ERROR, resp.status());
        assertNotNull(resp.errorMessage());
        assertTrue(resp.errorMessage().contains("Not leader"));
    }

    @Test
    @DisplayName("GET should return error when shard not found")
    void getShouldReturnErrorWhenShardNotFound() {
        when(shardManager.selectShardForKey(any())).thenReturn(null);
        CacheRequest req = createRequest(CacheRequest.Command.GET, "key-no-shard", null);
        CacheResponse resp = processor.process(req);
        assertNotNull(resp);
        assertEquals(CacheResponse.Status.ERROR, resp.status());
        assertTrue(resp.errorMessage().contains("No shard available"));
    }

    @Test
    @DisplayName("GET should return error when RaftNode not available")
    void getShouldReturnErrorWhenRaftNodeNotAvailable() {
        when(shardManager.getShard(SHARD_ID)).thenReturn(null);
        CacheRequest req = createRequest(CacheRequest.Command.GET, "key-no-raft", null);
        CacheResponse resp = processor.process(req);
        assertNotNull(resp);
        assertEquals(CacheResponse.Status.ERROR, resp.status());
        assertTrue(resp.errorMessage().contains("Shard not available"));
    }

    @Test
    @DisplayName("GET should return error when StateMachine not available")
    void getShouldReturnErrorWhenStateMachineNotAvailable() {
        when(shardManager.getStateMachine(SHARD_ID)).thenReturn(null);
        CacheRequest req = createRequest(CacheRequest.Command.GET, "key-no-sm", null);
        CacheResponse resp = processor.process(req);
        assertNotNull(resp);
        assertEquals(CacheResponse.Status.ERROR, resp.status());
        assertTrue(resp.errorMessage().contains("State machine not available"));
    }

    @Test
    @DisplayName("SET with null key should return error")
    void setWithNullKeyShouldReturnError() {
        CacheRequest req = createRequest(CacheRequest.Command.SET, null, "value");
        CacheResponse resp = processor.process(req);
        assertNotNull(resp);
        assertEquals(CacheResponse.Status.ERROR, resp.status());
        assertTrue(resp.errorMessage().contains("Key must not be null"));
        verifyNoInteractions(raftNode);
    }

    @Test
    @DisplayName("SET with blank key should return error")
    void setWithBlankKeyShouldReturnError() {
        CacheRequest req = createRequest(CacheRequest.Command.SET, "   ", "value");
        CacheResponse resp = processor.process(req);
        assertNotNull(resp);
        assertEquals(CacheResponse.Status.ERROR, resp.status());
        assertTrue(resp.errorMessage().contains("Key must not be null"));
        verifyNoInteractions(raftNode);
    }

    @Test
    @DisplayName("SET with null value should return error")
    void setWithNullValueShouldReturnError() {
        CacheRequest req = createRequest(CacheRequest.Command.SET, "key-null-value", null);
        CacheResponse resp = processor.process(req);
        assertNotNull(resp);
        assertEquals(CacheResponse.Status.ERROR, resp.status());
        assertTrue(resp.errorMessage().contains("Value must not be null"));
        verifyNoInteractions(raftNode);
    }

    @Test
    @DisplayName("DELETE with null key should return error")
    void deleteWithNullKeyShouldReturnError() {
        CacheRequest req = createRequest(CacheRequest.Command.DELETE, null, null);
        CacheResponse resp = processor.process(req);
        assertNotNull(resp);
        assertEquals(CacheResponse.Status.ERROR, resp.status());
        assertTrue(resp.errorMessage().contains("Key must not be null"));
        verifyNoInteractions(raftNode);
    }

    private CacheRequest createRequest(CacheRequest.Command command, String key, String value) {
        return new CacheRequest(
                "request-" + System.nanoTime(),
                System.currentTimeMillis(),
                command,
                key,
                value
        );
    }
}
