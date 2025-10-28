package com.iksanov.distributedcache.common.codec;

import com.iksanov.distributedcache.common.exception.InvalidCacheRequestException;
import com.iksanov.distributedcache.common.exception.SerializationException;
import com.iksanov.distributedcache.common.dto.CacheRequest;
import com.iksanov.distributedcache.common.dto.CacheResponse;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.CorruptedFrameException;
import io.netty.handler.codec.MessageToMessageCodec;

import java.nio.charset.StandardCharsets;
import java.util.List;

public final class CacheMessageCodec extends MessageToMessageCodec<ByteBuf, Object> {

    private static final byte PROTOCOL_VERSION = 1;
    private static final byte TYPE_REQUEST = 0;
    private static final byte TYPE_RESPONSE = 1;
    private static final int MAX_STRING_LENGTH = 10_000;

    @Override
    protected void encode(ChannelHandlerContext ctx, Object msg, List<Object> out) {
        ByteBuf buffer = ctx.alloc().buffer();
        buffer.writeByte(PROTOCOL_VERSION);
        if (msg instanceof CacheRequest request) {
            buffer.writeByte(TYPE_REQUEST);
            encodeRequest(buffer, request);
        } else if (msg instanceof CacheResponse response) {
            buffer.writeByte(TYPE_RESPONSE);
            encodeResponse(buffer, response);
        } else {
            throw new SerializationException("Unsupported message type: " + msg.getClass());
        }
        out.add(buffer);
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        if (in.readableBytes() < 2) throw new CorruptedFrameException("Not enough data to read header");
        byte version = in.readByte();
        if (version != PROTOCOL_VERSION) throw new CorruptedFrameException("Unsupported protocol version: " + version);

        byte messageType = in.readByte();
        switch (messageType) {
            case TYPE_REQUEST -> out.add(decodeRequest(in));
            case TYPE_RESPONSE -> out.add(decodeResponse(in));
            default -> throw new CorruptedFrameException("Unknown message type: " + messageType);
        }
    }

    private void encodeRequest(ByteBuf buffer, CacheRequest request) {
        writeString(buffer, request.requestId());
        buffer.writeLong(request.timestamp());
        buffer.writeByte((byte) request.command().ordinal());
        writeString(buffer, request.key());
        writeNullableString(buffer, request.value());
    }

    private void encodeResponse(ByteBuf buffer, CacheResponse response) {
        writeString(buffer, response.requestId());
        writeNullableString(buffer, response.value());
        buffer.writeByte((byte) response.status().ordinal());
        writeNullableString(buffer, response.errorMessage());
    }

    private CacheRequest decodeRequest(ByteBuf in) {
        String requestId = readString(in);
        long timestamp = readLongSafe(in);
        int commandOrdinal = readByteSafe(in);
        CacheRequest.Command command = safeCommandFromOrdinal(commandOrdinal);
        String key = readString(in);
        String value = readNullableString(in);
        return new CacheRequest(requestId, timestamp, command, key, value);
    }

    private CacheResponse decodeResponse(ByteBuf in) {
        String requestId = readString(in);
        String value = readNullableString(in);
        int statusOrdinal = readByteSafe(in);
        CacheResponse.Status status = safeStatusFromOrdinal(statusOrdinal);
        String error = readNullableString(in);
        return new CacheResponse(requestId, value, status, error);
    }

    private void writeString(ByteBuf buffer, String s) {
        byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        buffer.writeInt(bytes.length);
        buffer.writeBytes(bytes);
    }

    private void writeNullableString(ByteBuf buffer, String s) {
        if (s == null) {
            buffer.writeInt(-1);
        } else {
            writeString(buffer, s);
        }
    }

    private String readString(ByteBuf in) {
        int len = in.readInt();
        if (len < 0 || len > MAX_STRING_LENGTH) throw new SerializationException("Invalid string length: " + len);
        if (in.readableBytes() < len) throw new CorruptedFrameException("Not enough data for string (expected " + len + ")");
        byte[] bytes = new byte[len];
        in.readBytes(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private String readNullableString(ByteBuf in) {
        int len = in.readInt();
        if (len == -1) return null;
        if (len < 0 || len > MAX_STRING_LENGTH) throw new SerializationException("Invalid nullable string length: " + len);
        if (in.readableBytes() < len) throw new CorruptedFrameException("Not enough data for nullable string (expected " + len + ")");
        byte[] bytes = new byte[len];
        in.readBytes(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private int readByteSafe(ByteBuf in) {
        if (in.readableBytes() < 1) {
            throw new CorruptedFrameException("Unexpected end of frame when reading byte");
        }
        return in.readByte();
    }

    private long readLongSafe(ByteBuf in) {
        if (in.readableBytes() < Long.BYTES) {
            throw new CorruptedFrameException("Unexpected end of frame when reading long");
        }
        return in.readLong();
    }

    private CacheRequest.Command safeCommandFromOrdinal(int ordinal) {
        CacheRequest.Command[] values = CacheRequest.Command.values();
        if (ordinal < 0 || ordinal >= values.length) {
            throw new InvalidCacheRequestException("Invalid command ordinal: " + ordinal);
        }
        return values[ordinal];
    }

    private CacheResponse.Status safeStatusFromOrdinal(int ordinal) {
        CacheResponse.Status[] values = CacheResponse.Status.values();
        if (ordinal < 0 || ordinal >= values.length) {
            throw new InvalidCacheRequestException("Invalid status ordinal: " + ordinal);
        }
        return values[ordinal];
    }
}