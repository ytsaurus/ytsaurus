package tech.ytsaurus.client;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import javax.annotation.Nullable;

import NYT.NChunkClient.NProto.DataStatistics.TDataStatistics;
import tech.ytsaurus.client.rows.WireRowDeserializer;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.skiff.serialization.EntitySkiffSerializer;
import tech.ytsaurus.ysontree.YTreeNode;

public interface TableAttachmentReader<T> {
    @Nullable
    List<T> parse(@Nullable byte[] attachments) throws Exception;

    @Nullable
    List<T> parse(@Nullable byte[] attachments, int offset, int length) throws Exception;

    long getTotalRowCount();

    @Nullable
    TDataStatistics getDataStatistics();

    @Nullable
    TableSchema getCurrentReadSchema();

    /**
     * @deprecated don't use it explicitly
     */
    @Deprecated
    static <T> TableAttachmentReader<T> wireProtocol(WireRowDeserializer<T> deserializer) {
        return new TableAttachmentWireProtocolReader<>(deserializer);
    }

    static TableAttachmentReader<byte[]> byPass() {
        return new TableAttachmentByPassReader();
    }

    static TableAttachmentReader<ByteBuffer> byteBuffer() {
        return new TableAttachmentByteBufferReader();
    }

    static TableAttachmentReader<YTreeNode> ysonBinary() {
        return new TableAttachmentYsonReader();
    }

    static <T> TableAttachmentReader<T> skiff(EntitySkiffSerializer<T> serializer) {
        return new TableAttachmentSkiffReader<>(serializer);
    }
}

class TableAttachmentByPassReader implements TableAttachmentReader<byte[]> {
    @Override
    public List<byte[]> parse(@Nullable byte[] attachments) {
        if (attachments == null) {
            return null;
        } else {
            return Arrays.asList(attachments);
        }
    }

    @Override
    public List<byte[]> parse(@Nullable byte[] attachments, int offset, int length) {
        if (attachments == null) {
            return null;
        } else {
            if (offset == 0 && length == attachments.length) {
                return List.of(attachments);
            } else {
                return List.of(Arrays.copyOfRange(attachments, offset, length));
            }
        }
    }

    @Override
    public long getTotalRowCount() {
        return 0;
    }

    @Override
    public TDataStatistics getDataStatistics() {
        return null;
    }

    @Nullable
    @Override
    public TableSchema getCurrentReadSchema() {
        return null;
    }
}
