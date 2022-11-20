package tech.ytsaurus.client;

import ru.yandex.yt.ytclient.object.WireRowDeserializer;

/**
 * TableAttachmentReaderImpl is deprecated, use TableAttachmentWireProtocolReader instead
 */
@Deprecated
public class TableAttachmentReaderImpl<T> extends TableAttachmentWireProtocolReader<T> {
    public TableAttachmentReaderImpl(WireRowDeserializer<T> deserializer) {
        super(deserializer);
    }
}
