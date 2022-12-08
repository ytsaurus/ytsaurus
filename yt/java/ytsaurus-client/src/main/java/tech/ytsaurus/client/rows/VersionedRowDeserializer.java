package tech.ytsaurus.client.rows;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class VersionedRowDeserializer
        extends VersionedValueDeserializer
        implements WireVersionedRowDeserializer<VersionedRow> {
    private final List<WireColumnSchema> keyColumnSchema;
    private List<Long> writeTimestamps = Collections.emptyList();

    private List<Long> deleteTimestamps = Collections.emptyList();

    private List<UnversionedValue> keys = Collections.emptyList();
    private List<VersionedValue> values = Collections.emptyList();

    private final UnversionedValueDeserializer keyDeserializer;
    private final VersionedValueDeserializer valueDeserializer;

    public VersionedRowDeserializer(List<WireColumnSchema> keyColumnSchema) {
        this.keyColumnSchema = Objects.requireNonNull(keyColumnSchema);

        this.keyDeserializer = new UnversionedValueDeserializer() {
            @Override
            public UnversionedValue build() {
                final UnversionedValue value = super.build();
                keys.add(value);
                return value;
            }
        };
        this.valueDeserializer = new VersionedValueDeserializer() {
            @Override
            public VersionedValue build() {
                final VersionedValue value = super.build();
                values.add(value);
                return value;
            }
        };
    }


    @Override
    public WireValueDeserializer<?> keys(int keyCount) {
        this.keys = new ArrayList<>(keyCount);
        return this.keyDeserializer;
    }

    @Override
    public WireValueDeserializer<?> values(int valueCount) {
        this.values = new ArrayList<>(valueCount);
        return this.valueDeserializer;
    }

    @Override
    public VersionedRow onCompleteRow() {
        return new VersionedRow(writeTimestamps, deleteTimestamps, keys, values);
    }

    @Override
    public void onWriteTimestamps(List<Long> timestamps) {
        this.writeTimestamps = timestamps;
    }

    @Override
    public void onDeleteTimestamps(List<Long> timestamps) {
        this.deleteTimestamps = timestamps;
    }

    @Override
    public List<WireColumnSchema> getKeyColumnSchema() {
        return keyColumnSchema;
    }

}
