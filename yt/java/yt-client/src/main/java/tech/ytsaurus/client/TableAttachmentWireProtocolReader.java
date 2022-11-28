package tech.ytsaurus.client;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import tech.ytsaurus.client.rows.WireProtocolReader;
import tech.ytsaurus.client.rows.WireRowDeserializer;

class TableAttachmentWireProtocolReader<T> extends TableAttachmentRowsetReader<T> {
    private final WireRowDeserializer<T> deserializer;

    TableAttachmentWireProtocolReader(WireRowDeserializer<T> deserializer) {
        this.deserializer = Objects.requireNonNull(deserializer);
    }

    @Override
    protected List<T> parseMergedRow(ByteBuffer bb, int size) {
        byte[] data = new byte[size];
        bb.get(data);

        WireProtocolReader reader = new WireProtocolReader(Arrays.asList(data));

        Objects.requireNonNull(currentReadSchema);
        deserializer.updateSchema(currentReadSchema);

        int rowCount = reader.readRowCount();

        List<T> rows = new ArrayList<>(rowCount);

        for (int i = 0; i < rowCount; ++i) {
            rows.add(reader.readUnversionedRow(deserializer));
        }

        return rows;
    }
}
