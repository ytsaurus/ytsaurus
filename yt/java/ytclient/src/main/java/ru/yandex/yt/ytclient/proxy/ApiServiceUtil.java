package ru.yandex.yt.ytclient.proxy;

import java.time.Duration;
import java.util.List;

import ru.yandex.yt.ytclient.tables.ColumnSchema;
import ru.yandex.yt.ytclient.tables.ColumnValueType;
import ru.yandex.yt.ytclient.tables.TableSchema;
import ru.yandex.yt.ytclient.wire.UnversionedRow;
import ru.yandex.yt.ytclient.wire.UnversionedRowset;
import ru.yandex.yt.ytclient.wire.UnversionedValue;
import ru.yandex.yt.ytclient.wire.VersionedRow;
import ru.yandex.yt.ytclient.wire.VersionedRowset;
import ru.yandex.yt.ytclient.wire.WireColumnSchema;
import ru.yandex.yt.ytclient.wire.WireProtocol;
import ru.yandex.yt.ytclient.wire.WireProtocolReader;
import ru.yandex.yt.rpcproxy.ERowsetKind;
import ru.yandex.yt.rpcproxy.TRowsetDescriptor;

public class ApiServiceUtil {
    public static final long MICROS_PER_SECOND = 1_000_000L;

    /**
     * Конвертирует исходные сырые значения в значения колонок по указанной схеме
     */
    public static void convertKeyColumns(List<UnversionedValue> row, TableSchema schema, List<?> values) {
        for (int id = 0; id < schema.getKeyColumnsCount(); ++id) {
            ColumnSchema column = schema.getColumns().get(id);
            ColumnValueType type = column.getType();
            Object value = UnversionedValue.convertValueTo(values.get(id), type);
            if (value == null) {
                type = ColumnValueType.NULL;
            }
            row.add(new UnversionedValue(id, type, false, value));
        }
    }

    public static void convertValueColumns(List<UnversionedValue> row, TableSchema schema, List<?> values,
            boolean skipMissingValues, boolean aggregate)
    {
        for (int id = schema.getKeyColumnsCount(); id < schema.getColumns().size() && id < values.size(); ++id) {
            ColumnSchema column = schema.getColumns().get(id);
            Object inputValue = values.get(id);
            if (inputValue == null && skipMissingValues) {
                continue;
            }
            ColumnValueType type = column.getType();
            Object value = UnversionedValue.convertValueTo(inputValue, type);
            if (value == null) {
                type = ColumnValueType.NULL;
            }
            row.add(new UnversionedValue(id, type, aggregate, value));
        }
    }

    public static TableSchema deserializeRowsetSchema(TRowsetDescriptor descriptor) {
        TableSchema.Builder builder = new TableSchema.Builder().setUniqueKeys(false);
        for (TRowsetDescriptor.TColumnDescriptor columnDescriptor : descriptor.getColumnsList()) {
            String name = "";
            if (columnDescriptor.hasName()) {
                name = columnDescriptor.getName();
            }
            ColumnValueType type = ColumnValueType.NULL;
            if (columnDescriptor.hasType()) {
                type = ColumnValueType.fromValue(columnDescriptor.getType());
            }
            builder.addValue(name, type);
        }
        return builder.build();
    }

    public static UnversionedRowset deserializeUnversionedRowset(TRowsetDescriptor descriptor,
                                                                 List<byte[]> attachments)
    {
        if (descriptor.getWireFormatVersion() != WireProtocol.WIRE_FORMAT_VERSION) {
            throw new IllegalStateException("Cannot deserialize wire format" + descriptor.getWireFormatVersion() + ": "
                    + WireProtocol.WIRE_FORMAT_VERSION + " is required");
        }
        if (descriptor.getRowsetKind() != ERowsetKind.UNVERSIONED) {
            throw new IllegalStateException(
                    "Cannot deserialize " + descriptor.getRowsetKind() + ": UNVERSIONED is required");
        }
        TableSchema schema = deserializeRowsetSchema(descriptor);
        WireProtocolReader reader = new WireProtocolReader(attachments);
        List<UnversionedRow> rows = reader.readUnversionedRowset();
        return new UnversionedRowset(schema, rows);
    }

    public static VersionedRowset deserializeVersionedRowset(TRowsetDescriptor descriptor, List<byte[]> attachments) {
        if (descriptor.getWireFormatVersion() != WireProtocol.WIRE_FORMAT_VERSION) {
            throw new IllegalStateException("Cannot deserialize wire format" + descriptor.getWireFormatVersion() + ": "
                    + WireProtocol.WIRE_FORMAT_VERSION + " is required");
        }
        if (descriptor.getRowsetKind() != ERowsetKind.VERSIONED) {
            throw new IllegalStateException(
                    "Cannot deserialize " + descriptor.getRowsetKind() + ": UNVERSIONED is required");
        }
        TableSchema schema = deserializeRowsetSchema(descriptor);
        List<WireColumnSchema> schemaData = WireProtocolReader.makeSchemaData(schema);
        WireProtocolReader reader = new WireProtocolReader(attachments);
        List<VersionedRow> rows = reader.readVersionedRowset(schemaData);
        return new VersionedRowset(schema, rows);
    }

    public static TRowsetDescriptor makeRowsetDescriptor(TableSchema schema) {
        TRowsetDescriptor.Builder builder = TRowsetDescriptor.newBuilder();
        builder.setWireFormatVersion(WireProtocol.WIRE_FORMAT_VERSION);
        builder.setRowsetKind(ERowsetKind.UNVERSIONED);
        for (ColumnSchema column : schema.getColumns()) {
            builder.addColumnsBuilder()
                    .setName(column.getName())
                    .setType(column.getType().getValue());
        }
        return builder.build();
    }

    public static long durationToYtMicros(Duration duration) {
        long micros = Math.multiplyExact(duration.getSeconds(), MICROS_PER_SECOND);
        micros = Math.addExact(micros, duration.getNano() / 1000);
        return micros;
    }
}
