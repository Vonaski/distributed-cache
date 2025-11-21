package com.iksanov.distributedcache.common.codec;

import com.iksanov.distributedcache.common.dto.CacheRequest;
import com.iksanov.distributedcache.common.dto.CacheResponse;
import com.iksanov.distributedcache.common.exception.SerializationException;
import io.netty.buffer.ByteBuf;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.CorruptedFrameException;
import io.netty.handler.codec.DecoderException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive tests for {@link CacheMessageCodec}.
 * Covers request/response encoding & decoding, null handling, version validation,
 * and edge cases to ensure protocol correctness and stability.
 */
class CacheMessageCodecTest {

    @Test
    @DisplayName("CacheRequest should correctly encode and decode through the codec")
    void testEncodeDecodeCacheRequest() {
        CacheRequest request = new CacheRequest(
                "req-100",
                System.currentTimeMillis(),
                CacheRequest.Command.SET,
                "user:1",
                "John Doe"
        );

        EmbeddedChannel channel = new EmbeddedChannel(new CacheMessageCodec());
        assertTrue(channel.writeOutbound(request));
        ByteBuf encoded = channel.readOutbound();
        assertNotNull(encoded);
        assertTrue(encoded.readableBytes() > 0);
        assertTrue(channel.writeInbound(encoded.retain()));
        CacheRequest decoded = channel.readInbound();
        assertEquals(request.requestId(), decoded.requestId());
        assertEquals(request.key(), decoded.key());
        assertEquals(request.value(), decoded.value());
        assertEquals(request.command(), decoded.command());
    }

    @Test
    @DisplayName("CacheResponse (OK) should encode and decode correctly")
    void testEncodeDecodeCacheResponseOk() {
        CacheResponse response = CacheResponse.ok("id-1", "some-value");
        EmbeddedChannel channel = new EmbeddedChannel(new CacheMessageCodec());
        assertTrue(channel.writeOutbound(response));
        ByteBuf encoded = channel.readOutbound();
        assertTrue(channel.writeInbound(encoded.retain()));
        CacheResponse decoded = channel.readInbound();
        assertEquals(response.requestId(), decoded.requestId());
        assertEquals(response.value(), decoded.value());
        assertEquals(response.status(), decoded.status());
        assertNull(decoded.errorMessage());
    }

    @Test
    @DisplayName("CacheResponse (ERROR) should encode and decode correctly")
    void testEncodeDecodeCacheResponseError() {
        CacheResponse response = CacheResponse.error("id-2", "internal error");
        EmbeddedChannel channel = new EmbeddedChannel(new CacheMessageCodec());
        assertTrue(channel.writeOutbound(response));
        ByteBuf encoded = channel.readOutbound();
        assertTrue(channel.writeInbound(encoded.retain()));
        CacheResponse decoded = channel.readInbound();
        assertEquals(response.requestId(), decoded.requestId());
        assertEquals(CacheResponse.Status.ERROR, decoded.status());
        assertEquals("internal error", decoded.errorMessage());
        assertNull(decoded.value());
    }

    @Test
    @DisplayName("CacheResponse (NOT_FOUND) should encode and decode correctly")
    void testEncodeDecodeCacheResponseNotFound() {
        CacheResponse response = CacheResponse.notFound("id-3");
        EmbeddedChannel channel = new EmbeddedChannel(new CacheMessageCodec());
        assertTrue(channel.writeOutbound(response));
        ByteBuf encoded = channel.readOutbound();
        assertTrue(channel.writeInbound(encoded.retain()));
        CacheResponse decoded = channel.readInbound();
        assertEquals(CacheResponse.Status.NOT_FOUND, decoded.status());
        assertNull(decoded.value());
        assertNull(decoded.errorMessage());
    }

    @Test
    @DisplayName("Null values should be encoded and decoded correctly for both Request and Response")
    void testNullValuesInRequestAndResponse() {
        CacheRequest request = new CacheRequest("req-null", 1L, CacheRequest.Command.SET, "key", null);
        CacheResponse response = new CacheResponse("res-null", null, CacheResponse.Status.OK, null);
        EmbeddedChannel channel = new EmbeddedChannel(new CacheMessageCodec());
        assertTrue(channel.writeOutbound(request));
        ByteBuf encodedReq = channel.readOutbound();
        assertTrue(channel.writeInbound(encodedReq.retain()));
        CacheRequest decodedReq = channel.readInbound();
        assertNull(decodedReq.value());
        assertTrue(channel.writeOutbound(response));
        ByteBuf encodedRes = channel.readOutbound();
        assertTrue(channel.writeInbound(encodedRes.retain()));
        CacheResponse decodedRes = channel.readInbound();
        assertNull(decodedRes.value());
        assertNull(decodedRes.errorMessage());
    }

    @Test
    @DisplayName("Codec should handle large payloads correctly")
    void testLargePayload() {
        String largeValue = "x".repeat(5000);
        CacheRequest request = new CacheRequest(
                "large",
                System.currentTimeMillis(),
                CacheRequest.Command.SET,
                "key-large",
                largeValue
        );

        EmbeddedChannel channel = new EmbeddedChannel(new CacheMessageCodec());
        assertTrue(channel.writeOutbound(request));
        ByteBuf encoded = channel.readOutbound();
        assertTrue(channel.writeInbound(encoded.retain()));
        CacheRequest decoded = channel.readInbound();
        assertEquals(largeValue, decoded.value());
    }

    @Test
    @DisplayName("Codec should throw an exception for an unknown message type")
    void testInvalidTypeThrowsException() {
        EmbeddedChannel channel = new EmbeddedChannel(new CacheMessageCodec());
        ByteBuf bad = channel.alloc().buffer();
        bad.writeByte(1);
        bad.writeByte(99);
        assertThrows(CorruptedFrameException.class, () -> channel.writeInbound(bad));
    }

    @Test
    @DisplayName("Codec should throw an exception for invalid protocol version")
    void testInvalidVersionThrowsException() {
        EmbeddedChannel channel = new EmbeddedChannel(new CacheMessageCodec());
        ByteBuf bad = channel.alloc().buffer();
        bad.writeByte(42);
        bad.writeByte(0);
        assertThrows(CorruptedFrameException.class, () -> channel.writeInbound(bad));
    }

    @Test
    @DisplayName("Codec should throw an exception for invalid string length (negative)")
    void testInvalidStringLengthThrowsException() {
        EmbeddedChannel channel = new EmbeddedChannel(new CacheMessageCodec());
        ByteBuf bad = channel.alloc().buffer();
        bad.writeByte(1);
        bad.writeByte(0);
        bad.writeInt(-100);
        DecoderException ex = assertThrows(DecoderException.class, () -> channel.writeInbound(bad));
        assertInstanceOf(SerializationException.class, ex.getCause());
        assertTrue(ex.getCause().getMessage().contains("Invalid string length"));
    }

    @Test
    @DisplayName("Codec should handle empty key values gracefully")
    void testEmptyKeyInRequestHandledGracefully() {
        CacheRequest request = new CacheRequest("id-empty", System.currentTimeMillis(), CacheRequest.Command.GET, "", null);
        EmbeddedChannel channel = new EmbeddedChannel(new CacheMessageCodec());

        assertDoesNotThrow(() -> {
            channel.writeOutbound(request);
            ByteBuf encoded = channel.readOutbound();
            channel.writeInbound(encoded.retain());
            CacheRequest decoded = channel.readInbound();
            assertEquals("", decoded.key());
        });
    }
}
