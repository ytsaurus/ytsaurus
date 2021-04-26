package ru.yandex.yt.ytclient.object;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import ru.yandex.yt.ytclient.tables.TableSchema;
import ru.yandex.yt.ytclient.wire.VersionedRow;
import ru.yandex.yt.ytclient.wire.VersionedRowset;
import ru.yandex.yt.ytclient.wire.WireProtocolReader;

public class VersionedRowsetDeserializer
        extends VersionedRowDeserializer
        implements WireVersionedRowsetDeserializer<VersionedRow> {
    private final TableSchema schema;
    private List<VersionedRow> rows = Collections.emptyList();

    public VersionedRowsetDeserializer(TableSchema schema) {
        super(WireProtocolReader.makeSchemaData(schema));
        this.schema = Objects.requireNonNull(schema);
    }

    @Override
    public void setRowCount(int rowCount) {
        this.rows = new ArrayList<>(rowCount);
    }

    @Override
    public VersionedRow onCompleteRow() {
        VersionedRow row = super.onCompleteRow();
        this.rows.add(row);
        return row;
    }

    public VersionedRowset getRowset() {
        return new VersionedRowset(schema, this.rows);
    }
}
