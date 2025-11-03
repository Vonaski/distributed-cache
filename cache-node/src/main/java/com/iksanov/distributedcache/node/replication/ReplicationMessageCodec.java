package com.iksanov.distributedcache.node.replication;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.CorruptedFrameException;
import io.netty.handler.codec.MessageToMessageCodec;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Simple and efficient codec for serializing {@link ReplicationTask}.
 * <p>
 * Works together with {@link io.netty.handler.codec.LengthFieldPrepender} /
 * {@link io.netty.handler.codec.LengthFieldBasedFrameDecoder}.
 */
public class ReplicationMessageCodec extends MessageToMessageCodec<ByteBuf, ReplicationTask> {

    @Override
    protected void encode(ChannelHandlerContext ctx, ReplicationTask task, List<Object> out) {
        ByteBuf buf = ctx.alloc().buffer();
        buf.writeByte(task.operation() == ReplicationTask.Operation.SET ? 0 : 1);

        byte[] keyBytes = task.key().getBytes(StandardCharsets.UTF_8);
        buf.writeInt(keyBytes.length);
        buf.writeBytes(keyBytes);

        if (task.value() != null) {
            byte[] valueBytes = task.value().getBytes(StandardCharsets.UTF_8);
            buf.writeInt(valueBytes.length);
            buf.writeBytes(valueBytes);
        } else {
            buf.writeInt(-1);
        }

        buf.writeLong(task.timestamp());

        if (task.origin() != null) {
            byte[] originBytes = task.origin().getBytes(StandardCharsets.UTF_8);
            buf.writeInt(originBytes.length);
            buf.writeBytes(originBytes);
        } else {
            buf.writeInt(-1);
        }
        buf.writeLong(task.sequence());
        out.add(buf);
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf buf, List<Object> out) {
        int opByte = buf.readByte();
        ReplicationTask.Operation operation = (opByte == 0) ? ReplicationTask.Operation.SET : ReplicationTask.Operation.DELETE;

        int keyLen = buf.readInt();
        if (keyLen <= 0 || keyLen > 1024) throw new CorruptedFrameException("Invalid key length: " + keyLen);
        byte[] keyBytes = new byte[keyLen];
        buf.readBytes(keyBytes);
        String key = new String(keyBytes, StandardCharsets.UTF_8);

        int valueLen = buf.readInt();
        String value = null;
        if (valueLen >= 0) {
            if (valueLen > 64 * 1024) {
                throw new CorruptedFrameException("Value too long: " + valueLen);
            }
            if (valueLen > 0) {
                byte[] valueBytes = new byte[valueLen];
                buf.readBytes(valueBytes);
                value = new String(valueBytes, StandardCharsets.UTF_8);
            } else {
                value = "";
            }
        }

        long timestamp = buf.readLong();

        int originLen = buf.readInt();
        String origin = null;
        if (originLen >= 0) {
            if (originLen > 1024) {
                throw new CorruptedFrameException("Origin too long: " + originLen);
            }
            if (originLen > 0) {
                byte[] originBytes = new byte[originLen];
                buf.readBytes(originBytes);
                origin = new String(originBytes, StandardCharsets.UTF_8);
            } else {
                origin = "";
            }
        }
        long sequence = 0L;
        if (buf.readableBytes() >= 8) sequence = buf.readLong();
        out.add(new ReplicationTask(key, value, operation, timestamp, origin, sequence));
    }
}