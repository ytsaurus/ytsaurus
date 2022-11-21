package tech.ytsaurus.client;


import tech.ytsaurus.client.rows.WireRowDeserializer;
/**
 * TableAttachmentReaderImpl is deprecated, use TableAttachmentWireProtocolReader instead
 */
@Deprecated
public class TableAttachmentReaderImpl<T> extends TableAttachmentWireProtocolReader<T> {
    public TableAttachmentReaderImpl(WireRowDeserializer<T> deserializer) {
        super(deserializer);
    }
}
