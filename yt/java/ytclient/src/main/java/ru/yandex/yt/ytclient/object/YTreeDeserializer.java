package ru.yandex.yt.ytclient.object;

import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import ru.yandex.inside.yt.kosher.impl.ytree.builder.YTree;
import ru.yandex.inside.yt.kosher.impl.ytree.builder.YTreeBuilder;
import ru.yandex.inside.yt.kosher.impl.ytree.object.YTreeSerializer;
import ru.yandex.yt.ytclient.tables.ColumnSchema;
import ru.yandex.yt.ytclient.tables.ColumnValueType;
import ru.yandex.yt.ytclient.tables.TableSchema;

public class YTreeDeserializer<T> implements WireRowDeserializer<T>, WireValueDeserializer<Void> {
    private TableSchema schema;
    private String[] id2key;
    private YTreeBuilder builder = YTree.builder();
    private final YTreeSerializer<T> serializer;

    public YTreeDeserializer(YTreeSerializer<T> serializer) {
        this.serializer = Objects.requireNonNull(serializer);
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
    @Nonnull
    public WireValueDeserializer<?> onNewRow(int columnCount) {
        builder = YTree.builder().beginMap();
        return this;
    }

    @Override
    @Nonnull
    public T onCompleteRow() {
        return serializer.deserialize(builder.endMap().build().mapNode());
    }

    @Override
    @Nullable
    public T onNullRow() {
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
