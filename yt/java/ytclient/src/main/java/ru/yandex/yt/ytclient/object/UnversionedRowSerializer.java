package ru.yandex.yt.ytclient.object;

import java.util.Objects;

import ru.yandex.yt.ytclient.tables.TableSchema;
import ru.yandex.yt.ytclient.wire.UnversionedRow;
import ru.yandex.yt.ytclient.wire.UnversionedValue;

public class UnversionedRowSerializer implements WireRowSerializer<UnversionedRow> {
    private final TableSchema schema;

    public UnversionedRowSerializer(TableSchema schema) {
        this.schema = Objects.requireNonNull(schema);
    }

    @Override
    public TableSchema getSchema() {
        return schema;
    }

    @Override
    public void serializeRow(UnversionedRow row, WireProtocolWriteable writeable) {
        writeable.writeValueCount(row.getValues().size());
        for (UnversionedValue value : row.getValues()) {
            writeable.writeValueHeader(value.getId(), value.getType(), value.isAggregate(), value.getLength());
            final Object v = value.getValue();
            switch (value.getType()) {
                case INT64:
                case UINT64:
                    writeable.onInteger((Long) v);
                    break;
                case DOUBLE:
                    writeable.onDouble((Double) v);
                    break;
                case BOOLEAN:
                    writeable.onBoolean((Boolean) v);
                    break;
                case STRING:
                case ANY:
                    writeable.onBytes(value.bytesValue());
                    break;
            }

        }
    }
}
