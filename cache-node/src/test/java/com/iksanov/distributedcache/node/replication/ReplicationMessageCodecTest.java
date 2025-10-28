package com.iksanov.distributedcache.node.replication;

import io.netty.buffer.ByteBuf;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link ReplicationMessageCodec}.
 * <p>
 * Covers:
 *  - Encode ReplicationTask into ByteBuf
 *  - Decode ByteBuf back to ReplicationTask
 *  - Round-trip consistency
 *  - Null-safe encoding for value and origin
 */
public class ReplicationMessageCodecTest {

    @Test
    void shouldEncodeAndDecodeReplicationTask() {
        ReplicationTask original = ReplicationTask.ofSet("user:1", "Alice", "node-A");
        EmbeddedChannel channel = new EmbeddedChannel(new ReplicationMessageCodec());

        assertTrue(channel.writeOutbound(original));
        ByteBuf encoded = channel.readOutbound();

        assertTrue(channel.writeInbound(encoded.retain()));
        ReplicationTask decoded = channel.readInbound();

        assertEquals(original.key(), decoded.key());
        assertEquals(original.value(), decoded.value());
        assertEquals(original.operation(), decoded.operation());
        assertEquals(original.origin(), decoded.origin());
        assertTrue(decoded.timestamp() >= original.timestamp());

        channel.finishAndReleaseAll();
    }

    @Test
    void shouldHandleNullValueAndOrigin() {
        ReplicationTask original = new ReplicationTask("key", null, ReplicationTask.Operation.DELETE,
                System.currentTimeMillis(), null);

        EmbeddedChannel channel = new EmbeddedChannel(new ReplicationMessageCodec());

        assertTrue(channel.writeOutbound(original));
        ByteBuf encoded = channel.readOutbound();

        assertTrue(channel.writeInbound(encoded.retain()));
        ReplicationTask decoded = channel.readInbound();

        assertEquals(original.key(), decoded.key());
        assertNull(decoded.value());
        assertEquals(original.operation(), decoded.operation());
        assertNull(decoded.origin());

        channel.finishAndReleaseAll();
    }

    @Test
    void shouldSupportRoundTripConsistency() {
        ReplicationTask original = ReplicationTask.ofSet("session:42", "payload", "replica-1");

        EmbeddedChannel channel = new EmbeddedChannel(new ReplicationMessageCodec());

        assertTrue(channel.writeOutbound(original));
        ByteBuf encoded = channel.readOutbound();
        assertTrue(channel.writeInbound(encoded.retain()));
        ReplicationTask result = channel.readInbound();

        assertEquals(original.key(), result.key());
        assertEquals(original.value(), result.value());
        assertEquals(original.operation(), result.operation());
        assertEquals(original.origin(), result.origin());

        channel.finishAndReleaseAll();
    }
}
