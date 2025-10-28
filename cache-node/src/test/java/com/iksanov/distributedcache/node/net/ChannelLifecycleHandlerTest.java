package com.iksanov.distributedcache.node.net;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.net.InetSocketAddress;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link ChannelLifecycleHandler}.
 * <p>
 * Verifies correct behavior of channel lifecycle events:
 *  - channelActive() and channelInactive() logging and connection tracking
 *  - exceptionCaught() closes context and logs error
 *  - handlerAdded() correctly initializes internal state
 */
@ExtendWith(MockitoExtension.class)
class ChannelLifecycleHandlerTest {

    @Mock
    private ChannelHandlerContext ctx;
    @Mock
    private Channel channel;
    private ChannelLifecycleHandler handler;

    @BeforeEach
    void setUp() {
        handler = new ChannelLifecycleHandler();
        lenient().when(ctx.channel()).thenReturn(channel);
        lenient().when(channel.id()).thenReturn(mock(ChannelId.class));
        lenient().when(channel.remoteAddress()).thenReturn(new InetSocketAddress("127.0.0.1", 9000));
    }

    @Test
    @DisplayName("handlerAdded should not throw and log initialization")
    void handlerAddedShouldInitialize() {
        assertDoesNotThrow(() -> handler.handlerAdded(ctx),
                "handlerAdded should not throw any exception");
    }

    @Test
    @DisplayName("channelActive should log new connection and increment count")
    void channelActiveShouldIncrementConnections() {
        handler.channelActive(ctx);
        assertEquals(1, handler.getActiveConnections(),
                "After first activation, active connection count must be 1");

        handler.channelActive(ctx);
        assertEquals(2, handler.getActiveConnections(),
                "After second activation, active connection count must be 2");

        verify(channel, atLeastOnce()).remoteAddress();
    }

    @Test
    @DisplayName("channelInactive should log disconnect and decrement count safely")
    void channelInactiveShouldDecrementConnections() {
        handler.channelActive(ctx);
        handler.channelActive(ctx);

        assertEquals(2, handler.getActiveConnections(),
                "Precondition: should have 2 active connections");

        handler.channelInactive(ctx);
        assertEquals(1, handler.getActiveConnections(),
                "After inactive event, connection count must decrement by 1");

        handler.channelInactive(ctx);
        assertEquals(0, handler.getActiveConnections(),
                "Connection count should not go below 0");

        handler.channelInactive(ctx);
        assertEquals(0, handler.getActiveConnections(),
                "Multiple inactive calls must not cause negative counts");
    }

    @Test
    @DisplayName("exceptionCaught should log and close context safely")
    void exceptionCaughtShouldCloseContext() {
        Throwable cause = new RuntimeException("test-exception");

        handler.exceptionCaught(ctx, cause);

        verify(ctx, times(1)).close();
        verify(channel, atLeastOnce()).remoteAddress();
    }

    @Test
    @DisplayName("Multiple independent handlers maintain separate connection counts")
    void handlersShouldMaintainIndependentCounts() {
        ChannelLifecycleHandler h1 = new ChannelLifecycleHandler();
        ChannelLifecycleHandler h2 = new ChannelLifecycleHandler();

        h1.channelActive(ctx);
        h1.channelActive(ctx);
        h2.channelActive(ctx);

        assertEquals(2, h1.getActiveConnections(), "Handler1 should have 2 active");
        assertEquals(1, h2.getActiveConnections(), "Handler2 should have 1 active");

        h1.channelInactive(ctx);
        assertEquals(1, h1.getActiveConnections());
        assertEquals(1, h2.getActiveConnections());
    }
}
