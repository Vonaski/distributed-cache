package com.iksanov.distributedcache.node.consensus.transport;

import com.iksanov.distributedcache.common.exception.SerializationException;
import com.iksanov.distributedcache.node.consensus.model.AppendEntriesRequest;
import com.iksanov.distributedcache.node.consensus.model.AppendEntriesResponse;
import com.iksanov.distributedcache.node.consensus.model.Command;
import com.iksanov.distributedcache.node.consensus.model.LogEntry;
import com.iksanov.distributedcache.node.consensus.model.VoteRequest;
import com.iksanov.distributedcache.node.consensus.model.VoteResponse;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.CorruptedFrameException;
import io.netty.handler.codec.MessageToMessageCodec;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public final class RaftMessageCodec extends MessageToMessageCodec<ByteBuf, Object> {

    private static final byte PROTOCOL_VERSION = 1;
    private static final byte TYPE_VOTE_REQUEST = 1;
    private static final byte TYPE_VOTE_RESPONSE = 2;
    private static final byte TYPE_APPEND_ENTRIES = 3;
    private static final byte TYPE_APPEND_RESPONSE = 4;

    private static final int MAX_STRING_LENGTH = 10_000;
    private static final int MAX_PAYLOAD = 5 * 1024 * 1024;
    private static final int MAX_ENTRIES = 10_000;
    private static final int MAX_ENTRY_KEY_LEN = 1024;
    private static final int MAX_ENTRY_VALUE_LEN = 64 * 1024;

    @Override
    protected void encode(ChannelHandlerContext ctx, Object msg, List<Object> out) {
        Objects.requireNonNull(msg, "msg");
        ByteBuf buffer = ctx.alloc().buffer();
        buffer.writeByte(PROTOCOL_VERSION);

        try {
            switch (msg) {
                case VoteRequest req -> {
                    buffer.writeByte(TYPE_VOTE_REQUEST);
                    encodeVoteRequest(buffer, req);
                }
                case VoteResponse resp -> {
                    buffer.writeByte(TYPE_VOTE_RESPONSE);
                    encodeVoteResponse(buffer, resp);
                }
                case AppendEntriesRequest req -> {
                    buffer.writeByte(TYPE_APPEND_ENTRIES);
                    encodeAppendEntries(buffer, req);
                }
                case AppendEntriesResponse resp -> {
                    buffer.writeByte(TYPE_APPEND_RESPONSE);
                    encodeAppendResponse(buffer, resp);
                }
                default -> throw new SerializationException("Unknown message type: " + msg.getClass());
            }
            out.add(buffer);
        } catch (Throwable t) {
            buffer.release();
            throw t;
        }
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        if (in.readableBytes() < 2) throw new CorruptedFrameException("Not enough data to read header");
        byte version = in.readByte();
        if (version != PROTOCOL_VERSION) throw new CorruptedFrameException("Unsupported protocol version: " + version);
        byte type = in.readByte();
        try {
            switch (type) {
                case TYPE_VOTE_REQUEST -> out.add(decodeVoteRequest(in));
                case TYPE_VOTE_RESPONSE -> out.add(decodeVoteResponse(in));
                case TYPE_APPEND_ENTRIES -> out.add(decodeAppendEntries(in));
                case TYPE_APPEND_RESPONSE -> out.add(decodeAppendResponse(in));
                default -> throw new CorruptedFrameException("Unknown message type: " + type);
            }
        } catch (IndexOutOfBoundsException | IllegalArgumentException ex) {
            throw new CorruptedFrameException("Failed to decode message: " + ex.getMessage(), ex);
        }
    }

    private void encodeVoteRequest(ByteBuf buf, VoteRequest req) {
        buf.writeLong(req.term());
        writeString(buf, req.candidateId());
        buf.writeLong(req.lastLogIndex());
        buf.writeLong(req.lastLogTerm());
    }

    private VoteRequest decodeVoteRequest(ByteBuf buf) {
        ensureReadable(buf, Long.BYTES);
        long term = buf.readLong();
        String candidateId = readString(buf);
        ensureReadable(buf, Long.BYTES * 2);
        long lastLogIndex = buf.readLong();
        long lastLogTerm = buf.readLong();
        return new VoteRequest(term, candidateId, lastLogIndex, lastLogTerm);
    }

    private void encodeVoteResponse(ByteBuf buf, VoteResponse resp) {
        buf.writeLong(resp.term());
        buf.writeBoolean(resp.voteGranted());
    }

    private VoteResponse decodeVoteResponse(ByteBuf buf) {
        ensureReadable(buf, Long.BYTES + 1);
        long term = buf.readLong();
        boolean granted = buf.readBoolean();
        return new VoteResponse(term, granted);
    }

    private void encodeAppendEntries(ByteBuf buf, AppendEntriesRequest req) {
        buf.writeLong(req.term());
        writeString(buf, req.leaderId());
        buf.writeLong(req.prevLogIndex());
        buf.writeLong(req.prevLogTerm());
        buf.writeLong(req.leaderCommit());

        List<LogEntry> entries = req.entries();
        if (entries == null) entries = List.of();
        if (entries.size() > MAX_ENTRIES) throw new SerializationException("Too many entries: " + entries.size());
        buf.writeInt(entries.size());
        for (LogEntry entry : entries) {
            buf.writeLong(entry.index());
            buf.writeLong(entry.term());
            int ordinal = entry.command().type().ordinal();
            buf.writeByte((byte) ordinal);
            writeStringWithLimit(buf, entry.command().key(), MAX_ENTRY_KEY_LEN);
            writeNullableStringWithLimit(buf, entry.command().value(), MAX_ENTRY_VALUE_LEN);
        }
    }

    private AppendEntriesRequest decodeAppendEntries(ByteBuf buf) {
        ensureReadable(buf, Long.BYTES * 4); // term + prevLogIndex + prevLogTerm + leaderCommit (plus leaderId size later)
        long term = buf.readLong();
        String leaderId = readString(buf);
        ensureReadable(buf, Long.BYTES * 3);
        long prevLogIndex = buf.readLong();
        long prevLogTerm = buf.readLong();
        long leaderCommit = buf.readLong();
        ensureReadable(buf, Integer.BYTES);
        int size = buf.readInt();
        if (size < 0 || size > MAX_ENTRIES) throw new CorruptedFrameException("Invalid entries count: " + size);

        List<LogEntry> entries = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            ensureReadable(buf, Long.BYTES * 2 + 1 + Integer.BYTES);
            long index = buf.readLong();
            long entryTerm = buf.readLong();
            int cmdOrdinal = buf.readUnsignedByte();
            if (cmdOrdinal < 0 || cmdOrdinal >= Command.Type.values().length) throw new CorruptedFrameException("Invalid command type ordinal: " + cmdOrdinal);
            String key = readString(buf);
            if (key.length() > MAX_ENTRY_KEY_LEN) throw new CorruptedFrameException("Key too long");
            String value = readNullableString(buf);
            if (value != null && value.length() > MAX_ENTRY_VALUE_LEN) throw new CorruptedFrameException("Value too long");
            Command.Type type = Command.Type.values()[cmdOrdinal];
            Command cmd = new Command(type, key, value);
            entries.add(new LogEntry(index, entryTerm, cmd));
        }
        return new AppendEntriesRequest(term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit);
    }

    private void encodeAppendResponse(ByteBuf buf, AppendEntriesResponse resp) {
        buf.writeLong(resp.term());
        buf.writeBoolean(resp.success());
        buf.writeLong(resp.matchIndex());
    }

    private AppendEntriesResponse decodeAppendResponse(ByteBuf buf) {
        ensureReadable(buf, Long.BYTES + 1 + Long.BYTES);
        long term = buf.readLong();
        boolean success = buf.readBoolean();
        long matchIndex = buf.readLong();
        return new AppendEntriesResponse(term, success, matchIndex);
    }

    private void writeString(ByteBuf buf, String str) {
        if (str == null) throw new SerializationException("String cannot be null here");
        byte[] bytes = str.getBytes(StandardCharsets.UTF_8);
        if (bytes.length > MAX_STRING_LENGTH) throw new SerializationException("String too long: " + bytes.length);
        buf.writeInt(bytes.length);
        buf.writeBytes(bytes);
    }

    private void writeStringWithLimit(ByteBuf buf, String str, int maxLen) {
        if (str == null) throw new SerializationException("String cannot be null here");
        byte[] bytes = str.getBytes(StandardCharsets.UTF_8);
        if (bytes.length > maxLen) throw new SerializationException("String too long: " + bytes.length);
        buf.writeInt(bytes.length);
        buf.writeBytes(bytes);
    }

    private String readString(ByteBuf buf) {
        int len = buf.readInt();
        if (len < 0 || len > MAX_STRING_LENGTH) throw new CorruptedFrameException("Invalid string length: " + len);
        ensureReadable(buf, len);
        byte[] bytes = new byte[len];
        buf.readBytes(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private void writeNullableStringWithLimit(ByteBuf buf, String str, int maxLen) {
        if (str == null) {
            buf.writeInt(-1);
            return;
        }
        writeStringWithLimit(buf, str, maxLen);
    }

    private String readNullableString(ByteBuf buf) {
        int len = buf.readInt();
        if (len == -1) return null;
        if (len < 0 || len > MAX_STRING_LENGTH) throw new CorruptedFrameException("Invalid string length: " + len);
        ensureReadable(buf, len);
        byte[] bytes = new byte[len];
        buf.readBytes(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private static void ensureReadable(ByteBuf buf, int need) {
        if (buf.readableBytes() < need) throw new CorruptedFrameException("Not enough bytes: need=" + need + " available=" + buf.readableBytes());
    }
}