package ru.yandex.yt.ytclient.proxy.internal;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import ru.yandex.yt.ytclient.object.WireRowDeserializer;
import ru.yandex.yt.ytclient.wire.WireProtocolReader;

public class TableAttachmentWireProtocolReader<T> extends TableAttachmentRowsetReader<T> {
    private final WireRowDeserializer<T> deserializer;

    public TableAttachmentWireProtocolReader(WireRowDeserializer<T> deserializer) {
        this.deserializer = Objects.requireNonNull(deserializer);
    }

    @Override
    protected List<T> parseMergedRow(ByteBuffer bb, int size) {
        byte[] data = new byte[size];
        bb.get(data);

        WireProtocolReader reader = new WireProtocolReader(Arrays.asList(data));

        deserializer.updateSchema(currentReadSchema);

        int rowCount = reader.readRowCount();

        List<T> rows = new ArrayList<>(rowCount);

        for (int i = 0; i < rowCount; ++i) {
            rows.add(reader.readUnversionedRow(deserializer));
        }

        return rows;
    }
}
