package com.iksanov.distributedcache.node.replication;

import io.netty.buffer.ByteBuf;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.DecoderException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link ReplicationMessageCodec}.
 * <p>
 * Covers:
 *  - Encode/decode for SET and DELETE operations
 *  - Round-trip consistency
 *  - Null handling for value and origin
 *  - Edge cases (empty strings, large values)
 *  - Validation and error handling
 */
public class ReplicationMessageCodecTest {

    @Test
    @DisplayName("Should encode and decode SET task correctly")
    void shouldEncodeAndDecodeSetTask() {
        ReplicationTask original = ReplicationTask.ofSet("user:1", "Alice", "node-A", 0L);
        EmbeddedChannel channel = new EmbeddedChannel(new ReplicationMessageCodec());
        assertTrue(channel.writeOutbound(original));
        ByteBuf encoded = channel.readOutbound();
        assertNotNull(encoded);
        assertTrue(encoded.readableBytes() > 0);
        assertTrue(channel.writeInbound(encoded.retain()));
        ReplicationTask decoded = channel.readInbound();
        assertEquals(original.key(), decoded.key());
        assertEquals(original.value(), decoded.value());
        assertEquals(original.operation(), decoded.operation());
        assertEquals(original.origin(), decoded.origin());
        assertEquals(original.timestamp(), decoded.timestamp());
        channel.finishAndReleaseAll();
    }

    @Test
    @DisplayName("Should encode and decode DELETE task correctly")
    void shouldEncodeAndDecodeDeleteTask() {
        ReplicationTask original = ReplicationTask.ofDelete("session:42", "node-B", 0L);
        EmbeddedChannel channel = new EmbeddedChannel(new ReplicationMessageCodec());
        assertTrue(channel.writeOutbound(original));
        ByteBuf encoded = channel.readOutbound();
        assertTrue(channel.writeInbound(encoded.retain()));
        ReplicationTask decoded = channel.readInbound();
        assertEquals(original.key(), decoded.key());
        assertNull(decoded.value());
        assertEquals(ReplicationTask.Operation.DELETE, decoded.operation());
        assertEquals(original.origin(), decoded.origin());
        channel.finishAndReleaseAll();
    }

    @Test
    @DisplayName("Should handle null value in SET task")
    void shouldHandleNullValue() {
        ReplicationTask original = new ReplicationTask("key", null,
                ReplicationTask.Operation.SET, System.currentTimeMillis(), "node-A", 0L);
        EmbeddedChannel channel = new EmbeddedChannel(new ReplicationMessageCodec());
        assertTrue(channel.writeOutbound(original));
        ByteBuf encoded = channel.readOutbound();
        assertTrue(channel.writeInbound(encoded.retain()));
        ReplicationTask decoded = channel.readInbound();
        assertEquals(original.key(), decoded.key());
        assertNull(decoded.value());
        assertEquals(original.operation(), decoded.operation());
        channel.finishAndReleaseAll();
    }

    @Test
    @DisplayName("Should handle null origin")
    void shouldHandleNullOrigin() {
        ReplicationTask original = new ReplicationTask("key", "value",
                ReplicationTask.Operation.SET, System.currentTimeMillis(), null, 0L);
        EmbeddedChannel channel = new EmbeddedChannel(new ReplicationMessageCodec());
        assertTrue(channel.writeOutbound(original));
        ByteBuf encoded = channel.readOutbound();
        assertTrue(channel.writeInbound(encoded.retain()));
        ReplicationTask decoded = channel.readInbound();
        assertEquals(original.key(), decoded.key());
        assertEquals(original.value(), decoded.value());
        assertNull(decoded.origin());
        channel.finishAndReleaseAll();
    }

    @Test
    @DisplayName("Should handle both null value and null origin")
    void shouldHandleNullValueAndOrigin() {
        ReplicationTask original = new ReplicationTask("key", null,
                ReplicationTask.Operation.DELETE, System.currentTimeMillis(), null, 0L);
        EmbeddedChannel channel = new EmbeddedChannel(new ReplicationMessageCodec());
        assertTrue(channel.writeOutbound(original));
        ByteBuf encoded = channel.readOutbound();
        assertTrue(channel.writeInbound(encoded.retain()));
        ReplicationTask decoded = channel.readInbound();
        assertEquals(original.key(), decoded.key());
        assertNull(decoded.value());
        assertNull(decoded.origin());
        assertEquals(ReplicationTask.Operation.DELETE, decoded.operation());

        channel.finishAndReleaseAll();
    }

    @Test
    @DisplayName("Should preserve exact timestamp")
    void shouldPreserveTimestamp() {
        long customTimestamp = 1234567890123L;
        ReplicationTask original = new ReplicationTask("key", "value",
                ReplicationTask.Operation.SET, customTimestamp, "node-X", 0L);
        EmbeddedChannel channel = new EmbeddedChannel(new ReplicationMessageCodec());
        assertTrue(channel.writeOutbound(original));
        ByteBuf encoded = channel.readOutbound();
        assertTrue(channel.writeInbound(encoded.retain()));
        ReplicationTask decoded = channel.readInbound();
        assertEquals(customTimestamp, decoded.timestamp());
        channel.finishAndReleaseAll();
    }

    @Test
    @DisplayName("Should support round-trip consistency")
    void shouldSupportRoundTripConsistency() {
        ReplicationTask original = ReplicationTask.ofSet("session:42", "payload", "replica-1", 0L);
        EmbeddedChannel channel = new EmbeddedChannel(new ReplicationMessageCodec());
        assertTrue(channel.writeOutbound(original));
        ByteBuf encoded = channel.readOutbound();
        assertTrue(channel.writeInbound(encoded.retain()));
        ReplicationTask result = channel.readInbound();
        assertEquals(original.key(), result.key());
        assertEquals(original.value(), result.value());
        assertEquals(original.operation(), result.operation());
        assertEquals(original.origin(), result.origin());
        assertEquals(original.timestamp(), result.timestamp());
        channel.finishAndReleaseAll();
    }

    @Test
    @DisplayName("Should handle empty strings for value and origin")
    void shouldHandleEmptyStrings() {
        ReplicationTask original = new ReplicationTask("key", "",
                ReplicationTask.Operation.SET, System.currentTimeMillis(), "", 0L);
        EmbeddedChannel channel = new EmbeddedChannel(new ReplicationMessageCodec());
        assertTrue(channel.writeOutbound(original));
        ByteBuf encoded = channel.readOutbound();
        assertTrue(channel.writeInbound(encoded.retain()));
        ReplicationTask decoded = channel.readInbound();
        assertEquals("", decoded.value());
        assertEquals("", decoded.origin());
        channel.finishAndReleaseAll();
    }

    @Test
    @DisplayName("Should handle large values (within limit)")
    void shouldHandleLargeValues() {
        String largeValue = "x".repeat(50_000); // 50KB, under 64KB limit
        ReplicationTask original = ReplicationTask.ofSet("key", largeValue, "node-A", 0L);
        EmbeddedChannel channel = new EmbeddedChannel(new ReplicationMessageCodec());
        assertTrue(channel.writeOutbound(original));
        ByteBuf encoded = channel.readOutbound();
        assertTrue(channel.writeInbound(encoded.retain()));
        ReplicationTask decoded = channel.readInbound();
        assertEquals(largeValue, decoded.value());
        channel.finishAndReleaseAll();
    }

    @Test
    @DisplayName("Should reject key length > 1024")
    void shouldRejectTooLongKey() {
        EmbeddedChannel channel = new EmbeddedChannel(new ReplicationMessageCodec());
        ByteBuf buf = channel.alloc().buffer();
        buf.writeByte(0);
        buf.writeInt(2000);
        assertThrows(DecoderException.class, () -> channel.writeInbound(buf));
        channel.finishAndReleaseAll();
    }

    @Test
    @DisplayName("Should reject value length > 64KB")
    void shouldRejectTooLongValue() {
        EmbeddedChannel channel = new EmbeddedChannel(new ReplicationMessageCodec());
        ByteBuf buf = channel.alloc().buffer();
        buf.writeByte(0);
        buf.writeInt(3);
        buf.writeBytes("key".getBytes());
        buf.writeInt(100_000);
        assertThrows(DecoderException.class, () -> channel.writeInbound(buf));
        channel.finishAndReleaseAll();
    }

    @Test
    @DisplayName("Should reject origin length > 1024")
    void shouldRejectTooLongOrigin() {
        EmbeddedChannel channel = new EmbeddedChannel(new ReplicationMessageCodec());
        ByteBuf buf = channel.alloc().buffer();
        buf.writeByte(0);
        buf.writeInt(3);
        buf.writeBytes("key".getBytes());
        buf.writeInt(5);
        buf.writeBytes("value".getBytes());
        buf.writeLong(System.currentTimeMillis());
        buf.writeInt(2000);
        assertThrows(DecoderException.class, () -> channel.writeInbound(buf));
        channel.finishAndReleaseAll();
    }

    @Test
    @DisplayName("Should reject key length <= 0")
    void shouldRejectZeroOrNegativeKeyLength() {
        EmbeddedChannel channel = new EmbeddedChannel(new ReplicationMessageCodec());
        ByteBuf buf = channel.alloc().buffer();

        buf.writeByte(0);
        buf.writeInt(0);

        assertThrows(DecoderException.class, () -> channel.writeInbound(buf));

        channel.finishAndReleaseAll();
    }

    @Test
    @DisplayName("Should handle multiple tasks in sequence")
    void shouldHandleMultipleTasksInSequence() {
        EmbeddedChannel channel = new EmbeddedChannel(new ReplicationMessageCodec());

        ReplicationTask task1 = ReplicationTask.ofSet("k1", "v1", "node-A", 0L);
        ReplicationTask task2 = ReplicationTask.ofDelete("k2", "node-B", 0L);
        ReplicationTask task3 = ReplicationTask.ofSet("k3", "v3", "node-C", 0L);

        assertTrue(channel.writeOutbound(task1));
        assertTrue(channel.writeOutbound(task2));
        assertTrue(channel.writeOutbound(task3));

        ByteBuf encoded1 = channel.readOutbound();
        ByteBuf encoded2 = channel.readOutbound();
        ByteBuf encoded3 = channel.readOutbound();

        assertTrue(channel.writeInbound(encoded1.retain()));
        assertTrue(channel.writeInbound(encoded2.retain()));
        assertTrue(channel.writeInbound(encoded3.retain()));

        ReplicationTask decoded1 = channel.readInbound();
        ReplicationTask decoded2 = channel.readInbound();
        ReplicationTask decoded3 = channel.readInbound();

        assertEquals("k1", decoded1.key());
        assertEquals("k2", decoded2.key());
        assertEquals("k3", decoded3.key());

        assertEquals(ReplicationTask.Operation.SET, decoded1.operation());
        assertEquals(ReplicationTask.Operation.DELETE, decoded2.operation());
        assertEquals(ReplicationTask.Operation.SET, decoded3.operation());

        channel.finishAndReleaseAll();
    }

    @Test
    @DisplayName("Should encode DELETE operation with byte 1")
    void shouldEncodeDeleteOperationCorrectly() {
        ReplicationTask deleteTask = ReplicationTask.ofDelete("key", "node-A", 0L);
        EmbeddedChannel channel = new EmbeddedChannel(new ReplicationMessageCodec());

        assertTrue(channel.writeOutbound(deleteTask));
        ByteBuf encoded = channel.readOutbound();

        byte opByte = encoded.readByte();
        assertEquals(1, opByte, "DELETE operation should be encoded as byte 1");

        channel.finishAndReleaseAll();
    }

    @Test
    @DisplayName("Should encode SET operation with byte 0")
    void shouldEncodeSetOperationCorrectly() {
        ReplicationTask setTask = ReplicationTask.ofSet("key", "value", "node-A", 0L);
        EmbeddedChannel channel = new EmbeddedChannel(new ReplicationMessageCodec());

        assertTrue(channel.writeOutbound(setTask));
        ByteBuf encoded = channel.readOutbound();

        byte opByte = encoded.readByte();
        assertEquals(0, opByte, "SET operation should be encoded as byte 0");

        channel.finishAndReleaseAll();
    }

    @Test
    @DisplayName("Should handle special characters in strings")
    void shouldHandleSpecialCharacters() {
        String specialValue = "Hello \n\t\r";
        ReplicationTask original = ReplicationTask.ofSet("key", specialValue, "node-Ω", 0L);
        EmbeddedChannel channel = new EmbeddedChannel(new ReplicationMessageCodec());

        assertTrue(channel.writeOutbound(original));
        ByteBuf encoded = channel.readOutbound();

        assertTrue(channel.writeInbound(encoded.retain()));
        ReplicationTask decoded = channel.readInbound();

        assertEquals(specialValue, decoded.value());
        assertEquals("node-Ω", decoded.origin());

        channel.finishAndReleaseAll();
    }
}