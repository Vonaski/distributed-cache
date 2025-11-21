package com.iksanov.distributedcache.node.net;

import com.iksanov.distributedcache.common.codec.CacheMessageCodec;
import com.iksanov.distributedcache.common.dto.CacheResponse;
import com.iksanov.distributedcache.node.config.ApplicationConfig;
import com.iksanov.distributedcache.node.core.CacheStore;
import com.iksanov.distributedcache.node.metrics.NetMetrics;
import com.iksanov.distributedcache.node.replication.ReplicationManager;
import io.netty.channel.ChannelHandler;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.logging.LoggingHandler;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link NetServerInitializer}.
 * <p>
 * Uses a mocked SocketChannel whose pipeline() returns an EmbeddedChannel.pipeline().
 * This approach lets us exercise initChannel(SocketChannel) exactly as in production,
 * while observing the real ChannelPipeline contents.
 */
@ExtendWith(MockitoExtension.class)
class NetServerInitializerTest {

    @Mock
    CacheStore store;
    @Mock
    ReplicationManager replicationManager;
    @Mock
    NetMetrics netMetrics;
    @Mock
    SocketChannel socketChannel;
    private NetServerInitializer initializerWithReplication;
    private RequestProcessor requestProcessor;

    @BeforeEach
    void setUp() {
        requestProcessor = new RequestProcessor(store, replicationManager, ApplicationConfig.NodeRole.MASTER, netMetrics, null);
        initializerWithReplication = new NetServerInitializer(requestProcessor, 1024, netMetrics);
    }

    @Test
    @DisplayName("initChannel should add handlers in expected order (by type)")
    void shouldAddExpectedHandlersInOrder() {
        EmbeddedChannel embedded = new EmbeddedChannel();
        when(socketChannel.pipeline()).thenReturn(embedded.pipeline());
        initializerWithReplication.initChannel(socketChannel);
        List<String> handlerTypes = embedded.pipeline().names().stream()
                .map(name -> embedded.pipeline().get(name))
                .filter(h -> h != null)
                .map(h -> h.getClass().getSimpleName())
                .filter(name -> !name.contains("HeadContext") && !name.contains("TailContext"))
                .collect(Collectors.toList());

        List<String> expectedTypes = List.of(
                "ChannelLifecycleHandler",
                "LoggingHandler",
                "LengthFieldBasedFrameDecoder",
                "LengthFieldPrepender",
                "CacheMessageCodec",
                "NetConnectionHandler"
        );
        assertTrue(handlerTypes.containsAll(expectedTypes), "Pipeline must contain all expected handler types");
        assertEquals(expectedTypes, handlerTypes.subList(0, expectedTypes.size()), "Handlers must be in correct order");
    }

    @Test
    @DisplayName("All handlers should be of correct types")
    void shouldUseCorrectHandlerTypes() {
        EmbeddedChannel embedded = new EmbeddedChannel();
        when(socketChannel.pipeline()).thenReturn(embedded.pipeline());
        initializerWithReplication.initChannel(socketChannel);
        assertNotNull(findByType(embedded, ChannelLifecycleHandler.class), "ChannelLifecycleHandler must be present");
        assertNotNull(findByType(embedded, LoggingHandler.class), "LoggingHandler must be present");
        assertNotNull(findByType(embedded, LengthFieldBasedFrameDecoder.class), "LengthFieldBasedFrameDecoder must be present");
        assertNotNull(findByType(embedded, LengthFieldPrepender.class), "LengthFieldPrepender must be present");
        assertNotNull(findByType(embedded, CacheMessageCodec.class), "CacheMessageCodec must be present");
        assertNotNull(findByType(embedded, NetConnectionHandler.class), "NetConnectionHandler must be present");
    }

    @Test
    @DisplayName("Pipeline should process valid messages without throwing")
    void pipelineShouldBeOperational() {
        EmbeddedChannel embedded = new EmbeddedChannel();
        when(socketChannel.pipeline()).thenReturn(embedded.pipeline());
        initializerWithReplication.initChannel(socketChannel);
        assertTrue(embedded.isOpen(), "Channel should be open");
        CacheResponse testResponse = CacheResponse.ok("req-1", "pong");
        assertDoesNotThrow(() -> embedded.writeOutbound(testResponse), "Pipeline must accept supported outbound messages");
        assertDoesNotThrow(() -> embedded.writeInbound(testResponse), "Pipeline must accept supported inbound messages");
    }

    private static ChannelHandler findByType(EmbeddedChannel ch, Class<?> type) {
        return ch.pipeline().names().stream()
                .map(ch.pipeline()::get)
                .filter(type::isInstance)
                .findFirst()
                .orElse(null);
    }
}
