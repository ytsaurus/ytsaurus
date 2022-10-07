package ru.yandex.yt.ytclient.proxy.internal;

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
