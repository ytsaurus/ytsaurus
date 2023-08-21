package tech.ytsaurus.client.rows;

import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import tech.ytsaurus.core.rows.YTreeSerializer;
import tech.ytsaurus.core.tables.ColumnSchema;
import tech.ytsaurus.core.tables.ColumnValueType;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeBuilder;


public class YTreeDeserializer<T> implements WireRowsetDeserializer<T>, WireValueDeserializer<Void> {
    private TableSchema schema;
    private String[] id2key;
    private YTreeBuilder builder = YTree.builder();
    private final YTreeSerializer<T> serializer;
    private final ConsumerSource<T> consumer;

    public YTreeDeserializer(YTreeSerializer<T> serializer) {
        this(serializer, (unused) -> {
        });
    }

    public YTreeDeserializer(YTreeSerializer<T> serializer, @Nullable ConsumerSource<T> consumer) {
        this.serializer = Objects.requireNonNull(serializer);
        this.consumer = Objects.requireNonNull(consumer);
    }

    public void updateSchema(@Nonnull TableSchema schema) {
        if (this.schema == null || !this.schema.equals(schema)) {
            int index = 0;

            id2key = new String[schema.getColumns().size()];

            for (ColumnSchema column : schema.getColumns()) {
                id2key[index++] = column.getName();
            }

            this.schema = schema;
        }
    }

    @Override
    public void setRowCount(int rowCount) {
        consumer.setRowCount(rowCount);
    }

    @Override
    @Nonnull
    public WireValueDeserializer<?> onNewRow(int columnCount) {
        builder = YTree.builder().beginMap();
        return this;
    }

    @Override
    @Nonnull
    public T onCompleteRow() {
        T row = serializer.deserialize(builder.endMap().build().mapNode());
        consumer.accept(row);
        return row;
    }

    @Override
    @Nullable
    public T onNullRow() {
        consumer.accept(null);
        return null;
    }

    @Override
    public void setId(int id) {
        if (id >= id2key.length) {
            throw new IllegalStateException();
        }

        builder.key(id2key[id]);
    }

    @Override
    public void setType(ColumnValueType type) {

    }

    @Override
    public void setAggregate(boolean aggregate) {

    }

    @Override
    public void setTimestamp(long timestamp) {

    }

    @Override
    public Void build() {
        return null;
    }

    @Override
    public void onEntity() {
        builder.entity();
    }

    @Override
    public void onInteger(long value) {
        builder.value(value);
    }

    @Override
    public void onBoolean(boolean value) {
        builder.value(value);
    }

    @Override
    public void onDouble(double value) {
        builder.value(value);
    }

    @Override
    public void onBytes(byte[] bytes) {
        builder.value(bytes);
    }
}
