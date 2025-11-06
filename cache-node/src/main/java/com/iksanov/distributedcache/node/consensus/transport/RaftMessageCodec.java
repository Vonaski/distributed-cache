package com.iksanov.distributedcache.node.consensus.transport;

import com.iksanov.distributedcache.node.consensus.model.*;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.CorruptedFrameException;
import io.netty.handler.codec.MessageToMessageCodec;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Binary codec for Raft messages with correlation id.
 * <p>
 * Wire format:
 * [type:byte] [idLen:int] [idBytes] [term:long] [payload...]
 * <p>
 * Types:
 * 1 - VoteRequest      -> [candidateIdLen:int][candidateIdBytes]
 * 2 - VoteResponse     -> [voteGranted:byte]
 * 3 - HeartbeatRequest -> [leaderIdLen:int][leaderIdBytes]
 * 4 - HeartbeatResponse-> [success:byte]
 * <p>
 * Decoding produces MessageEnvelope instances: (id, payloadObject)
 */
public class RaftMessageCodec extends MessageToMessageCodec<ByteBuf, Object> {

    public static final byte TYPE_VOTE_REQ = 1;
    public static final byte TYPE_VOTE_RESP = 2;
    public static final byte TYPE_HEARTBEAT_REQ = 3;
    public static final byte TYPE_HEARTBEAT_RESP = 4;
    private static final int MAX_STRING_LENGTH = 10_000;
    private static final int MIN_HEADER_SIZE = 1 + 4 + 8; // type + idLen + term
    private static final byte BOOLEAN_TRUE = 1;
    private static final byte BOOLEAN_FALSE = 0;

    public record MessageEnvelope(String id, Object msg) {
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, Object msg, List<Object> out) {
        ByteBuf buf = ctx.alloc().buffer(256, 4096);
        try {
            switch (msg) {
                case CorrelatedVoteRequest cvr -> {
                    buf.writeByte(TYPE_VOTE_REQ);
                    writeString(buf, cvr.id);
                    buf.writeLong(cvr.request.term());
                    writeString(buf, cvr.request.candidateId());
                }
                case CorrelatedVoteResponse cvrsp -> {
                    buf.writeByte(TYPE_VOTE_RESP);
                    writeString(buf, cvrsp.id);
                    buf.writeLong(cvrsp.response.term());
                    buf.writeByte(cvrsp.response.voteGranted() ? BOOLEAN_TRUE : BOOLEAN_FALSE);
                }
                case CorrelatedHeartbeatRequest chr -> {
                    buf.writeByte(TYPE_HEARTBEAT_REQ);
                    writeString(buf, chr.id);
                    buf.writeLong(chr.request.term());
                    writeString(buf, chr.request.leaderId());
                }
                case CorrelatedHeartbeatResponse chrsp -> {
                    buf.writeByte(TYPE_HEARTBEAT_RESP);
                    writeString(buf, chrsp.id);
                    buf.writeLong(chrsp.response.term());
                    buf.writeByte(chrsp.response.success() ? BOOLEAN_TRUE : BOOLEAN_FALSE);
                }
                default -> {
                    buf.release();
                    throw new IllegalArgumentException("Unsupported outbound type: " + msg.getClass());
                }
            }
            out.add(buf);
        } catch (Exception e) {
            buf.release();
            throw e;
        }
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        if (in.readableBytes() < MIN_HEADER_SIZE) throw new CorruptedFrameException("Frame too short: " + in.readableBytes() + " bytes");
        byte type = in.readByte();
        if (type < TYPE_VOTE_REQ || type > TYPE_HEARTBEAT_RESP) throw new CorruptedFrameException("Unknown message type: " + type);
        String id = readString(in);
        if (in.readableBytes() < 8) throw new CorruptedFrameException("Not enough bytes for term field");
        long term = in.readLong();
        switch (type) {
            case TYPE_VOTE_REQ -> {
                String candidate = readString(in);
                VoteRequest vr = new VoteRequest(term, candidate);
                out.add(new MessageEnvelope(id, vr));
            }
            case TYPE_VOTE_RESP -> {
                if (in.readableBytes() < 1) throw new CorruptedFrameException("Not enough bytes for voteGranted field");
                boolean granted = in.readByte() == BOOLEAN_TRUE;
                out.add(new MessageEnvelope(id, new VoteResponse(term, granted)));
            }
            case TYPE_HEARTBEAT_REQ -> {
                String leaderId = readString(in);
                HeartbeatRequest hr = new HeartbeatRequest(term, leaderId);
                out.add(new MessageEnvelope(id, hr));
            }
            case TYPE_HEARTBEAT_RESP -> {
                if (in.readableBytes() < 1) throw new CorruptedFrameException("Not enough bytes for success field");
                boolean success = in.readByte() == BOOLEAN_TRUE;
                out.add(new MessageEnvelope(id, new HeartbeatResponse(term, success)));
            }
            default -> throw new CorruptedFrameException("Unknown type: " + type);
        }
    }

    private static void writeString(ByteBuf buf, String s) {
        if (s == null) {
            buf.writeInt(-1);
            return;
        }
        byte[] b = s.getBytes(StandardCharsets.UTF_8);
        if (b.length > MAX_STRING_LENGTH) throw new IllegalArgumentException("String too long: " + b.length + " bytes");
        buf.writeInt(b.length);
        buf.writeBytes(b);
    }

    private static String readString(ByteBuf buf) {
        if (buf.readableBytes() < 4) throw new CorruptedFrameException("Not enough bytes for string length");
        int len = buf.readInt();
        if (len < 0) return null;
        if (len > MAX_STRING_LENGTH) throw new CorruptedFrameException("String too long: " + len + " bytes");
        if (buf.readableBytes() < len) throw new CorruptedFrameException("Not enough bytes for string content: expected " + len + ", available " + buf.readableBytes());
        byte[] b = new byte[len];
        buf.readBytes(b);
        return new String(b, StandardCharsets.UTF_8);
    }

    public record CorrelatedVoteRequest(String id, VoteRequest request) {
    }

    public record CorrelatedVoteResponse(String id, VoteResponse response) {
    }

    public record CorrelatedHeartbeatRequest(String id, HeartbeatRequest request) {
    }

    public record CorrelatedHeartbeatResponse(String id, HeartbeatResponse response) {
    }
}