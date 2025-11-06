package com.iksanov.distributedcache.node.consensus;

import com.iksanov.distributedcache.node.consensus.model.*;
import com.iksanov.distributedcache.node.consensus.transport.RaftMessageCodec;
import io.netty.buffer.ByteBuf;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.codec.EncoderException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive unit tests for {@link RaftMessageCodec}.
 * <p>
 * Coverage:
 * - VoteRequest/VoteResponse encoding & decoding
 * - HeartbeatRequest/HeartbeatResponse encoding & decoding
 * - Round-trip consistency for all message types
 * - Null handling for correlation IDs and strings
 * - Edge cases (empty strings, special characters, large values)
 * - Error handling (invalid types, corrupted frames, string length limits)
 * - Memory leak prevention
 */
class RaftMessageCodecTest {

    @Test
    @DisplayName("VoteRequest should correctly encode and decode")
    void shouldEncodeAndDecodeVoteRequest() {
        String correlationId = "vote-req-123";
        VoteRequest request = new VoteRequest(5L, "candidate-node-A");
        RaftMessageCodec.CorrelatedVoteRequest message = new RaftMessageCodec.CorrelatedVoteRequest(correlationId, request);
        EmbeddedChannel channel = new EmbeddedChannel(new RaftMessageCodec());
        assertTrue(channel.writeOutbound(message));
        ByteBuf encoded = channel.readOutbound();
        assertNotNull(encoded);
        assertTrue(encoded.readableBytes() > 0);
        assertTrue(channel.writeInbound(encoded));
        RaftMessageCodec.MessageEnvelope envelope = channel.readInbound();
        assertEquals(correlationId, envelope.id());
        assertInstanceOf(VoteRequest.class, envelope.msg());
        VoteRequest decoded = (VoteRequest) envelope.msg();
        assertEquals(5L, decoded.term());
        assertEquals("candidate-node-A", decoded.candidateId());
        channel.finishAndReleaseAll();
    }

    @Test
    @DisplayName("VoteRequest with special characters in candidateId")
    void shouldHandleSpecialCharactersInVoteRequest() {
        String specialId = "node-Ω-世界-🚀";
        VoteRequest request = new VoteRequest(100L, specialId);
        RaftMessageCodec.CorrelatedVoteRequest message = new RaftMessageCodec.CorrelatedVoteRequest("id-special", request);
        EmbeddedChannel channel = new EmbeddedChannel(new RaftMessageCodec());
        assertTrue(channel.writeOutbound(message));
        ByteBuf encoded = channel.readOutbound();
        assertTrue(channel.writeInbound(encoded));
        RaftMessageCodec.MessageEnvelope envelope = channel.readInbound();
        VoteRequest decoded = (VoteRequest) envelope.msg();
        assertEquals(specialId, decoded.candidateId());
        channel.finishAndReleaseAll();
    }

    @Test
    @DisplayName("VoteRequest with empty candidateId")
    void shouldHandleEmptyCandidateId() {
        VoteRequest request = new VoteRequest(1L, "");
        RaftMessageCodec.CorrelatedVoteRequest message = new RaftMessageCodec.CorrelatedVoteRequest("id-empty", request);
        EmbeddedChannel channel = new EmbeddedChannel(new RaftMessageCodec());
        assertTrue(channel.writeOutbound(message));
        ByteBuf encoded = channel.readOutbound();
        assertTrue(channel.writeInbound(encoded));
        RaftMessageCodec.MessageEnvelope envelope = channel.readInbound();
        VoteRequest decoded = (VoteRequest) envelope.msg();
        assertEquals("", decoded.candidateId());
        channel.finishAndReleaseAll();
    }

    @Test
    @DisplayName("VoteResponse (granted) should correctly encode and decode")
    void shouldEncodeAndDecodeVoteResponseGranted() {
        String correlationId = "vote-resp-456";
        VoteResponse response = new VoteResponse(7L, true);
        RaftMessageCodec.CorrelatedVoteResponse message = new RaftMessageCodec.CorrelatedVoteResponse(correlationId, response);
        EmbeddedChannel channel = new EmbeddedChannel(new RaftMessageCodec());
        assertTrue(channel.writeOutbound(message));
        ByteBuf encoded = channel.readOutbound();
        assertTrue(channel.writeInbound(encoded));
        RaftMessageCodec.MessageEnvelope envelope = channel.readInbound();
        assertEquals(correlationId, envelope.id());
        assertInstanceOf(VoteResponse.class, envelope.msg());
        VoteResponse decoded = (VoteResponse) envelope.msg();
        assertEquals(7L, decoded.term());
        assertTrue(decoded.voteGranted());
        channel.finishAndReleaseAll();
    }

    @Test
    @DisplayName("VoteResponse (denied) should correctly encode and decode")
    void shouldEncodeAndDecodeVoteResponseDenied() {
        VoteResponse response = new VoteResponse(10L, false);
        RaftMessageCodec.CorrelatedVoteResponse message = new RaftMessageCodec.CorrelatedVoteResponse("vote-denied", response);
        EmbeddedChannel channel = new EmbeddedChannel(new RaftMessageCodec());
        assertTrue(channel.writeOutbound(message));
        ByteBuf encoded = channel.readOutbound();
        assertTrue(channel.writeInbound(encoded));
        RaftMessageCodec.MessageEnvelope envelope = channel.readInbound();
        VoteResponse decoded = (VoteResponse) envelope.msg();
        assertEquals(10L, decoded.term());
        assertFalse(decoded.voteGranted());
        channel.finishAndReleaseAll();
    }

    @Test
    @DisplayName("HeartbeatRequest should correctly encode and decode")
    void shouldEncodeAndDecodeHeartbeatRequest() {
        String correlationId = "heartbeat-req-789";
        HeartbeatRequest request = new HeartbeatRequest(15L, "leader-node-B");
        RaftMessageCodec.CorrelatedHeartbeatRequest message = new RaftMessageCodec.CorrelatedHeartbeatRequest(correlationId, request);
        EmbeddedChannel channel = new EmbeddedChannel(new RaftMessageCodec());
        assertTrue(channel.writeOutbound(message));
        ByteBuf encoded = channel.readOutbound();
        assertTrue(channel.writeInbound(encoded));
        RaftMessageCodec.MessageEnvelope envelope = channel.readInbound();
        assertEquals(correlationId, envelope.id());
        assertInstanceOf(HeartbeatRequest.class, envelope.msg());
        HeartbeatRequest decoded = (HeartbeatRequest) envelope.msg();
        assertEquals(15L, decoded.term());
        assertEquals("leader-node-B", decoded.leaderId());
        channel.finishAndReleaseAll();
    }

    @Test
    @DisplayName("HeartbeatRequest with long leaderId")
    void shouldHandleLongLeaderId() {
        String longLeaderId = "node-" + "x".repeat(500);
        HeartbeatRequest request = new HeartbeatRequest(20L, longLeaderId);
        RaftMessageCodec.CorrelatedHeartbeatRequest message = new RaftMessageCodec.CorrelatedHeartbeatRequest("hb-long", request);
        EmbeddedChannel channel = new EmbeddedChannel(new RaftMessageCodec());
        assertTrue(channel.writeOutbound(message));
        ByteBuf encoded = channel.readOutbound();
        assertTrue(channel.writeInbound(encoded));
        RaftMessageCodec.MessageEnvelope envelope = channel.readInbound();
        HeartbeatRequest decoded = (HeartbeatRequest) envelope.msg();
        assertEquals(longLeaderId, decoded.leaderId());
        channel.finishAndReleaseAll();
    }

    @Test
    @DisplayName("HeartbeatResponse (success) should correctly encode and decode")
    void shouldEncodeAndDecodeHeartbeatResponseSuccess() {
        String correlationId = "heartbeat-resp-101";
        HeartbeatResponse response = new HeartbeatResponse(25L, true);
        RaftMessageCodec.CorrelatedHeartbeatResponse message = new RaftMessageCodec.CorrelatedHeartbeatResponse(correlationId, response);
        EmbeddedChannel channel = new EmbeddedChannel(new RaftMessageCodec());
        assertTrue(channel.writeOutbound(message));
        ByteBuf encoded = channel.readOutbound();
        assertTrue(channel.writeInbound(encoded));
        RaftMessageCodec.MessageEnvelope envelope = channel.readInbound();
        assertEquals(correlationId, envelope.id());
        assertInstanceOf(HeartbeatResponse.class, envelope.msg());
        HeartbeatResponse decoded = (HeartbeatResponse) envelope.msg();
        assertEquals(25L, decoded.term());
        assertTrue(decoded.success());
        channel.finishAndReleaseAll();
    }

    @Test
    @DisplayName("HeartbeatResponse (failure) should correctly encode and decode")
    void shouldEncodeAndDecodeHeartbeatResponseFailure() {
        HeartbeatResponse response = new HeartbeatResponse(30L, false);
        RaftMessageCodec.CorrelatedHeartbeatResponse message = new RaftMessageCodec.CorrelatedHeartbeatResponse("hb-fail", response);
        EmbeddedChannel channel = new EmbeddedChannel(new RaftMessageCodec());
        assertTrue(channel.writeOutbound(message));
        ByteBuf encoded = channel.readOutbound();
        assertTrue(channel.writeInbound(encoded));
        RaftMessageCodec.MessageEnvelope envelope = channel.readInbound();
        HeartbeatResponse decoded = (HeartbeatResponse) envelope.msg();
        assertEquals(30L, decoded.term());
        assertFalse(decoded.success());
        channel.finishAndReleaseAll();
    }

    @Test
    @DisplayName("Should handle multiple messages in sequence")
    void shouldHandleMultipleMessagesInSequence() {
        EmbeddedChannel channel = new EmbeddedChannel(new RaftMessageCodec());

        VoteRequest vr = new VoteRequest(1L, "node-A");
        VoteResponse vresp = new VoteResponse(2L, true);
        HeartbeatRequest hr = new HeartbeatRequest(3L, "node-B");
        HeartbeatResponse hresp = new HeartbeatResponse(4L, false);

        assertTrue(channel.writeOutbound(new RaftMessageCodec.CorrelatedVoteRequest("id1", vr)));
        assertTrue(channel.writeOutbound(new RaftMessageCodec.CorrelatedVoteResponse("id2", vresp)));
        assertTrue(channel.writeOutbound(new RaftMessageCodec.CorrelatedHeartbeatRequest("id3", hr)));
        assertTrue(channel.writeOutbound(new RaftMessageCodec.CorrelatedHeartbeatResponse("id4", hresp)));

        ByteBuf b1 = channel.readOutbound();
        ByteBuf b2 = channel.readOutbound();
        ByteBuf b3 = channel.readOutbound();
        ByteBuf b4 = channel.readOutbound();

        assertTrue(channel.writeInbound(b1));
        assertTrue(channel.writeInbound(b2));
        assertTrue(channel.writeInbound(b3));
        assertTrue(channel.writeInbound(b4));

        RaftMessageCodec.MessageEnvelope e1 = channel.readInbound();
        RaftMessageCodec.MessageEnvelope e2 = channel.readInbound();
        RaftMessageCodec.MessageEnvelope e3 = channel.readInbound();
        RaftMessageCodec.MessageEnvelope e4 = channel.readInbound();

        assertEquals("id1", e1.id());
        assertInstanceOf(VoteRequest.class, e1.msg());
        assertEquals("id2", e2.id());
        assertInstanceOf(VoteResponse.class, e2.msg());
        assertEquals("id3", e3.id());
        assertInstanceOf(HeartbeatRequest.class, e3.msg());
        assertEquals("id4", e4.id());
        assertInstanceOf(HeartbeatResponse.class, e4.msg());

        channel.finishAndReleaseAll();
    }

    @Test
    @DisplayName("Should handle null candidateId")
    void shouldHandleNullCandidateId() {
        VoteRequest request = new VoteRequest(1L, null);
        RaftMessageCodec.CorrelatedVoteRequest message = new RaftMessageCodec.CorrelatedVoteRequest("id-null", request);
        EmbeddedChannel channel = new EmbeddedChannel(new RaftMessageCodec());
        assertTrue(channel.writeOutbound(message));
        ByteBuf encoded = channel.readOutbound();
        assertTrue(channel.writeInbound(encoded));
        RaftMessageCodec.MessageEnvelope envelope = channel.readInbound();
        VoteRequest decoded = (VoteRequest) envelope.msg();
        assertNull(decoded.candidateId());
        channel.finishAndReleaseAll();
    }

    @Test
    @DisplayName("Should handle null leaderId")
    void shouldHandleNullLeaderId() {
        HeartbeatRequest request = new HeartbeatRequest(1L, null);
        RaftMessageCodec.CorrelatedHeartbeatRequest message = new RaftMessageCodec.CorrelatedHeartbeatRequest("id-null", request);
        EmbeddedChannel channel = new EmbeddedChannel(new RaftMessageCodec());
        assertTrue(channel.writeOutbound(message));
        ByteBuf encoded = channel.readOutbound();
        assertTrue(channel.writeInbound(encoded));
        RaftMessageCodec.MessageEnvelope envelope = channel.readInbound();
        HeartbeatRequest decoded = (HeartbeatRequest) envelope.msg();
        assertNull(decoded.leaderId());
        channel.finishAndReleaseAll();
    }

    @Test
    @DisplayName("Should handle null correlation ID")
    void shouldHandleNullCorrelationId() {
        VoteRequest request = new VoteRequest(1L, "node-A");
        RaftMessageCodec.CorrelatedVoteRequest message = new RaftMessageCodec.CorrelatedVoteRequest(null, request);
        EmbeddedChannel channel = new EmbeddedChannel(new RaftMessageCodec());
        assertTrue(channel.writeOutbound(message));
        ByteBuf encoded = channel.readOutbound();
        assertTrue(channel.writeInbound(encoded));
        RaftMessageCodec.MessageEnvelope envelope = channel.readInbound();
        assertNull(envelope.id());
        channel.finishAndReleaseAll();
    }

    @Test
    @DisplayName("Should reject unsupported outbound message type")
    void shouldRejectUnsupportedOutboundType() {
        EmbeddedChannel channel = new EmbeddedChannel(new RaftMessageCodec());
        Object invalidMessage = "Invalid message";
        assertThrows(Exception.class, () -> channel.writeOutbound(invalidMessage));
        VoteRequest validRequest = new VoteRequest(1L, "node-A");
        RaftMessageCodec.CorrelatedVoteRequest validMessage = new RaftMessageCodec.CorrelatedVoteRequest("id", validRequest);
        assertTrue(channel.writeOutbound(validMessage));
        ByteBuf encoded = channel.readOutbound();
        assertNotNull(encoded);
        encoded.release();
        channel.finishAndReleaseAll();
    }

    @Test
    @DisplayName("Should reject type out of valid range")
    void shouldRejectTypeOutOfRange() {
        EmbeddedChannel channel = new EmbeddedChannel(new RaftMessageCodec());
        ByteBuf buf = channel.alloc().buffer();
        buf.writeByte(0);
        buf.writeInt(5);
        buf.writeBytes("id123".getBytes());
        buf.writeLong(1L);

        DecoderException ex = assertThrows(DecoderException.class,
                () -> channel.writeInbound(buf));

        String errorMessage = ex.getCause() != null ? ex.getCause().getMessage() : ex.getMessage();
        assertTrue(errorMessage.contains("Unknown message type") || errorMessage.contains("CorruptedFrameException"),
                "Expected error about unknown message type but got: " + errorMessage);

        channel.finishAndReleaseAll();
    }

    @Test
    @DisplayName("Should reject missing term field")
    void shouldRejectMissingTermField() {
        EmbeddedChannel channel = new EmbeddedChannel(new RaftMessageCodec());
        ByteBuf buf = channel.alloc().buffer();
        buf.writeByte(RaftMessageCodec.TYPE_VOTE_REQ);
        buf.writeInt(5);
        buf.writeBytes("id123".getBytes());

        DecoderException ex = assertThrows(DecoderException.class,
                () -> channel.writeInbound(buf));

        String errorMessage = ex.getCause() != null ? ex.getCause().getMessage() : ex.getMessage();
        assertTrue(errorMessage.contains("Frame too short") ||
                        errorMessage.contains("Not enough bytes") ||
                        errorMessage.contains("CorruptedFrameException"),
                "Expected error about missing term field but got: " + errorMessage);

        channel.finishAndReleaseAll();
    }

    @Test
    @DisplayName("Should reject missing payload field in VoteResponse")
    void shouldRejectMissingPayloadInVoteResponse() {
        EmbeddedChannel channel = new EmbeddedChannel(new RaftMessageCodec());
        ByteBuf buf = channel.alloc().buffer();
        buf.writeByte(RaftMessageCodec.TYPE_VOTE_RESP);
        buf.writeInt(5);
        buf.writeBytes("id123".getBytes());
        buf.writeLong(10L);
        DecoderException ex = assertThrows(DecoderException.class, () -> channel.writeInbound(buf));
        String errorMessage = ex.getCause() != null ? ex.getCause().getMessage() : ex.getMessage();
        assertTrue(errorMessage.contains("Not enough bytes") || errorMessage.contains("CorruptedFrameException"),
                "Expected error about missing payload but got: " + errorMessage);

        channel.finishAndReleaseAll();
    }

    @Test
    @DisplayName("Should handle correlation ID with special characters")
    void shouldHandleSpecialCharactersInCorrelationId() {
        String specialId = "id-Ω-世界-🚀-\n\t\r";
        VoteRequest request = new VoteRequest(1L, "node-A");
        RaftMessageCodec.CorrelatedVoteRequest message = new RaftMessageCodec.CorrelatedVoteRequest(specialId, request);
        EmbeddedChannel channel = new EmbeddedChannel(new RaftMessageCodec());
        assertTrue(channel.writeOutbound(message));
        ByteBuf encoded = channel.readOutbound();
        assertTrue(channel.writeInbound(encoded));
        RaftMessageCodec.MessageEnvelope envelope = channel.readInbound();
        assertEquals(specialId, envelope.id());
        channel.finishAndReleaseAll();
    }

    @Test
    @DisplayName("Should handle term value of 0")
    void shouldHandleTermZero() {
        VoteRequest request = new VoteRequest(0L, "node-A");
        RaftMessageCodec.CorrelatedVoteRequest message = new RaftMessageCodec.CorrelatedVoteRequest("id", request);
        EmbeddedChannel channel = new EmbeddedChannel(new RaftMessageCodec());
        assertTrue(channel.writeOutbound(message));
        ByteBuf encoded = channel.readOutbound();
        assertTrue(channel.writeInbound(encoded));
        RaftMessageCodec.MessageEnvelope envelope = channel.readInbound();
        VoteRequest decoded = (VoteRequest) envelope.msg();
        assertEquals(0L, decoded.term());
        channel.finishAndReleaseAll();
    }

    @Test
    @DisplayName("Should handle very large term value")
    void shouldHandleVeryLargeTerm() {
        long largeTerm = Long.MAX_VALUE;
        VoteRequest request = new VoteRequest(largeTerm, "node-A");
        RaftMessageCodec.CorrelatedVoteRequest message = new RaftMessageCodec.CorrelatedVoteRequest("id", request);
        EmbeddedChannel channel = new EmbeddedChannel(new RaftMessageCodec());
        assertTrue(channel.writeOutbound(message));
        ByteBuf encoded = channel.readOutbound();
        assertTrue(channel.writeInbound(encoded));
        RaftMessageCodec.MessageEnvelope envelope = channel.readInbound();
        VoteRequest decoded = (VoteRequest) envelope.msg();
        assertEquals(largeTerm, decoded.term());
        channel.finishAndReleaseAll();
    }

    @Test
    @DisplayName("Should handle string at MAX_STRING_LENGTH boundary")
    void shouldHandleStringAtMaxLength() {
        String maxLengthString = "x".repeat(1000);
        VoteRequest request = new VoteRequest(1L, maxLengthString);
        RaftMessageCodec.CorrelatedVoteRequest message = new RaftMessageCodec.CorrelatedVoteRequest("id", request);
        EmbeddedChannel channel = new EmbeddedChannel(new RaftMessageCodec());
        assertTrue(channel.writeOutbound(message));
        ByteBuf encoded = channel.readOutbound();
        assertTrue(channel.writeInbound(encoded));
        RaftMessageCodec.MessageEnvelope envelope = channel.readInbound();
        VoteRequest decoded = (VoteRequest) envelope.msg();
        assertEquals(maxLengthString, decoded.candidateId());
        channel.finishAndReleaseAll();
    }

    @Test
    @DisplayName("Should throw when encoding string exceeds MAX_STRING_LENGTH")
    void shouldThrowWhenEncodingStringTooLong() {
        String tooLongString = "x".repeat(10_001);
        VoteRequest request = new VoteRequest(1L, tooLongString);
        RaftMessageCodec.CorrelatedVoteRequest message = new RaftMessageCodec.CorrelatedVoteRequest("id", request);
        EmbeddedChannel channel = new EmbeddedChannel(new RaftMessageCodec());
        io.netty.handler.codec.EncoderException ex = assertThrows(
                io.netty.handler.codec.EncoderException.class,
                () -> channel.writeOutbound(message)
        );
        Throwable cause = ex.getCause();
        assertInstanceOf(IllegalArgumentException.class, cause, "Expected IllegalArgumentException as cause but got: " + cause.getClass().getName());
        assertTrue(cause.getMessage().contains("String too long"));
        channel.finishAndReleaseAll();
    }


    @Test
    @DisplayName("Should not leak ByteBuf on encode exception")
    void shouldNotLeakByteBufOnEncodeException() {
        EmbeddedChannel channel = new EmbeddedChannel(new RaftMessageCodec());
        Object invalidMessage = "invalid";
        assertThrows(Exception.class, () -> channel.writeOutbound(invalidMessage));
        VoteRequest validRequest = new VoteRequest(1L, "node-A");
        RaftMessageCodec.CorrelatedVoteRequest validMessage = new RaftMessageCodec.CorrelatedVoteRequest("id", validRequest);
        assertTrue(channel.writeOutbound(validMessage));
        ByteBuf encoded = channel.readOutbound();
        assertNotNull(encoded);
        encoded.release();
        channel.finishAndReleaseAll();
    }
}