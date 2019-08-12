package ru.yandex.yt.ytclient.wire;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

import ru.yandex.inside.yt.kosher.impl.ytree.builder.YTree;
import ru.yandex.inside.yt.kosher.impl.ytree.builder.YTreeBuilder;
import ru.yandex.inside.yt.kosher.impl.ytree.serialization.YTreeConsumer;
import ru.yandex.inside.yt.kosher.ytree.YTreeMapNode;
import ru.yandex.yt.ytclient.tables.TableSchema;

public class UnversionedRow {
    private final List<UnversionedValue> values;

    public UnversionedRow(List<UnversionedValue> values) {
        this.values = Objects.requireNonNull(values);
    }

    public List<UnversionedValue> getValues() {
        return Collections.unmodifiableList(values);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof UnversionedRow)) {
            return false;
        }

        UnversionedRow that = (UnversionedRow) o;

        return values.equals(that.values);
    }

    @Override
    public int hashCode() {
        return values.hashCode();
    }

    @Override
    public String toString() {
        return "UnversionedRow{" +
                "values=" + values +
                '}';
    }

    public void writeTo(YTreeConsumer consumer, TableSchema schema) {
        writeTo(consumer, schema, false);
    }

    public void writeTo(YTreeConsumer consumer, TableSchema schema, boolean ignoreSystemColumns) {
        consumer.onBeginMap();
        for (UnversionedValue value : values)  {
            int index = value.getId();
            String name = schema.getColumnName(index);

            if (ignoreSystemColumns && name.startsWith("$")) {
                continue;
            }

            consumer.onKeyedItem(name);
            value.writeTo(consumer);
        }
        consumer.onEndMap();
    }

    public YTreeMapNode toYTreeMap(TableSchema schema) {
        return toYTreeMap(schema, false);
    }

    public YTreeMapNode toYTreeMap(TableSchema schema, boolean ignoreSystemColumns) {
        YTreeBuilder builder = YTree.builder();
        writeTo(builder, schema, ignoreSystemColumns);
        return builder.build().mapNode();
    }
}
