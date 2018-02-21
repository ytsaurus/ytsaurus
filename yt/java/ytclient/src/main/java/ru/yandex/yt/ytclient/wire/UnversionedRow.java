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
        consumer.onBeginMap();
        for (int index = 0; index < values.size(); index++) {
            if (index >= schema.getColumns().size()) {
                break;
            }
            String name = schema.getColumnName(index);
            consumer.onKeyedItem(name);
            values.get(index).writeTo(consumer);
        }
        consumer.onEndMap();
    }

    public YTreeMapNode toYTreeMap(TableSchema schema) {
        YTreeBuilder builder = YTree.builder();
        writeTo(builder, schema);
        return builder.build().mapNode();
    }
}
