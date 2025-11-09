package com.iksanov.distributedcache.node.net;

import com.iksanov.distributedcache.node.metrics.NetMetrics;
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
import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link ChannelLifecycleHandler}.
 */
@ExtendWith(MockitoExtension.class)
class ChannelLifecycleHandlerTest {

    @Mock
    private ChannelHandlerContext ctx;
    @Mock
    private Channel channel;
    @Mock
    private NetMetrics metrics;

    private ChannelLifecycleHandler handler;

    @BeforeEach
    void setUp() {
        handler = new ChannelLifecycleHandler(metrics);
        lenient().when(ctx.channel()).thenReturn(channel);
        lenient().when(channel.id()).thenReturn(mock(ChannelId.class));
        lenient().when(channel.remoteAddress()).thenReturn(new InetSocketAddress("127.0.0.1", 9000));
    }

    @Test
    @DisplayName("channelActive should increment metrics and log connection")
    void channelActiveShouldIncrementMetrics() {
        handler.channelActive(ctx);
        verify(metrics, times(1)).incrementConnections();
        verify(channel, atLeastOnce()).remoteAddress();
        verify(ctx).fireChannelActive();
    }

    @Test
    @DisplayName("channelInactive should increment closed connection metrics")
    void channelInactiveShouldIncrementClosedMetrics() {
        handler.channelInactive(ctx);
        verify(metrics, times(1)).incrementClosedConnections();
        verify(channel, atLeastOnce()).remoteAddress();
        verify(ctx).fireChannelInactive();
    }

    @Test
    @DisplayName("exceptionCaught should increment error metric and close context")
    void exceptionCaughtShouldIncrementErrorAndCloseContext() {
        Throwable cause = new RuntimeException("test-error");
        handler.exceptionCaught(ctx, cause);
        verify(metrics, times(1)).incrementErrors();
        verify(channel, atLeastOnce()).remoteAddress();
        verify(ctx).close();
    }

    @Test
    @DisplayName("Multiple independent handlers share same metrics instance")
    void multipleHandlersShareSameMetrics() {
        ChannelLifecycleHandler h1 = new ChannelLifecycleHandler(metrics);
        ChannelLifecycleHandler h2 = new ChannelLifecycleHandler(metrics);

        h1.channelActive(ctx);
        h2.channelInactive(ctx);

        verify(metrics, times(1)).incrementConnections();
        verify(metrics, times(1)).incrementClosedConnections();
    }
}
