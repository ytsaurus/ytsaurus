package tech.ytsaurus.client.request;

import java.nio.ByteBuffer;

import tech.ytsaurus.client.TableAttachmentReader;
import tech.ytsaurus.client.rows.WireRowDeserializer;
import tech.ytsaurus.rpcproxy.ERowsetFormat;
import tech.ytsaurus.ysontree.YTreeNode;

public class ReadSerializationContext<T> extends SerializationContext<T> {

    public ReadSerializationContext(TableAttachmentReader<T> attachmentReader) {
        this.attachmentReader = attachmentReader;
    }

    public ReadSerializationContext(WireRowDeserializer<T> deserializer) {
        this(TableAttachmentReader.wireProtocol(deserializer));
    }

    private ReadSerializationContext(ERowsetFormat rowsetFormat, TableAttachmentReader<T> attachmentReader) {
        this.rowsetFormat = rowsetFormat;
        this.attachmentReader = attachmentReader;
    }

    private ReadSerializationContext(Format format, TableAttachmentReader<T> attachmentReader) {
        this.format = format;
        this.rowsetFormat = ERowsetFormat.RF_FORMAT;
        this.attachmentReader = attachmentReader;
    }

    public static ReadSerializationContext<ByteBuffer> binaryArrow() {
        return new ReadSerializationContext<>(ERowsetFormat.RF_ARROW, TableAttachmentReader.byteBuffer());
    }

    public static ReadSerializationContext<YTreeNode> ysonBinary() {
        return new ReadSerializationContext<>(Format.ysonBinary(), TableAttachmentReader.ysonBinary());
    }
}
