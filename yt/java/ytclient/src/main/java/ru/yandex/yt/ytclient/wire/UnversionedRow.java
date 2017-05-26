package ru.yandex.yt.ytclient.wire;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

import ru.yandex.yt.ytclient.tables.TableSchema;
import ru.yandex.yt.ytclient.ytree.YTreeBuilder;
import ru.yandex.yt.ytclient.ytree.YTreeConsumer;
import ru.yandex.yt.ytclient.ytree.YTreeMapNode;

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
        YTreeBuilder builder = new YTreeBuilder();
        writeTo(builder, schema);
        return (YTreeMapNode) builder.build();
    }
}
