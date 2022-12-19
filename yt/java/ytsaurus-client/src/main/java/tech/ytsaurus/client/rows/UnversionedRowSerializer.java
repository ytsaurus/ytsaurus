package tech.ytsaurus.client.rows;

import java.util.Objects;


import tech.ytsaurus.core.tables.TableSchema;

public class UnversionedRowSerializer implements WireRowSerializer<UnversionedRow> {
    private final TableSchema schema;

    public UnversionedRowSerializer() {
        this(new TableSchema.Builder()
                .setUniqueKeys(false)
                .build() /* unused */);
    }

    public UnversionedRowSerializer(TableSchema schema) {
        this.schema = Objects.requireNonNull(schema);
    }


    @Override
    public TableSchema getSchema() {
        return schema;
    }

    private int getValueId(UnversionedValue value, int[] idMapping) {
        return idMapping == null
                ? value.getId()
                : idMapping[value.getId()];
    }

    @Override
    public void serializeRow(
            UnversionedRow row,
            WireProtocolWriteable writeable,
            boolean keyFieldsOnly,
            boolean aggregate,
            int[] idMapping
    ) {
        // keyFieldsOnly is not supported for unversioned rows
        writeable.writeValueCount(row.getValues().size());
        for (UnversionedValue value : row.getValues()) {
            writeable.writeValueHeader(
                    getValueId(value, idMapping),
                    value.getType(),
                    value.isAggregate(),
                    value.getLength()
            );
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
                case COMPOSITE:
                case ANY:
                    writeable.onBytes(value.bytesValue());
                    break;
                default:
                    break;
            }
        }
    }
}
