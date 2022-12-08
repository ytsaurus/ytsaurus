package tech.ytsaurus.client.rows;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.yson.YsonConsumer;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeBuilder;
import tech.ytsaurus.ysontree.YTreeMapNode;


public class VersionedRow {
    private final List<Long> writeTimestamps;
    private final List<Long> deleteTimestamps;
    private final List<UnversionedValue> keys;
    private final List<VersionedValue> values;

    public VersionedRow(
            List<Long> writeTimestamps,
            List<Long> deleteTimestamps,
            List<UnversionedValue> keys,
            List<VersionedValue> values
    ) {
        this.writeTimestamps = Objects.requireNonNull(writeTimestamps);
        this.deleteTimestamps = Objects.requireNonNull(deleteTimestamps);
        this.keys = Objects.requireNonNull(keys);
        this.values = Objects.requireNonNull(values);
    }

    public List<Long> getWriteTimestamps() {
        return Collections.unmodifiableList(writeTimestamps);
    }

    public List<Long> getDeleteTimestamps() {
        return Collections.unmodifiableList(deleteTimestamps);
    }

    public List<UnversionedValue> getKeys() {
        return Collections.unmodifiableList(keys);
    }

    public List<VersionedValue> getValues() {
        return Collections.unmodifiableList(values);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof VersionedRow)) {
            return false;
        }

        VersionedRow that = (VersionedRow) o;

        if (!writeTimestamps.equals(that.writeTimestamps)) {
            return false;
        }
        if (!deleteTimestamps.equals(that.deleteTimestamps)) {
            return false;
        }
        if (!keys.equals(that.keys)) {
            return false;
        }
        return values.equals(that.values);
    }

    @Override
    public int hashCode() {
        int result = writeTimestamps.hashCode();
        result = 31 * result + deleteTimestamps.hashCode();
        result = 31 * result + keys.hashCode();
        result = 31 * result + values.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "VersionedRow{" +
                "writeTimestamps=" + writeTimestamps +
                ", deleteTimestamps=" + deleteTimestamps +
                ", keys=" + keys +
                ", values=" + values +
                '}';
    }

    public void writeTo(YsonConsumer consumer, TableSchema schema) {
        consumer.onBeginAttributes();
        consumer.onKeyedItem("write_timestamps");
        consumer.onBeginList();
        for (long writeTimestamp : writeTimestamps) {
            consumer.onListItem();
            consumer.onUnsignedInteger(writeTimestamp);
        }
        consumer.onEndList();
        consumer.onKeyedItem("delete_timestamps");
        consumer.onBeginList();
        for (long deleteTimestamp : deleteTimestamps) {
            consumer.onListItem();
            consumer.onUnsignedInteger(deleteTimestamp);
        }
        consumer.onEndList();
        consumer.onEndAttributes();

        consumer.onBeginMap();
        for (UnversionedValue key : keys) {
            String name = schema.getColumnName(key.getId());
            consumer.onKeyedItem(name);
            key.writeTo(consumer);
        }
        int lastId = -1;
        for (VersionedValue value : values) {
            int id = value.getId();
            if (lastId != id) {
                if (lastId != -1) {
                    consumer.onEndList();
                }
                String name = schema.getColumnName(id);
                consumer.onKeyedItem(name);
                consumer.onBeginList();
                lastId = id;
            }
            consumer.onListItem();
            value.writeTo(consumer);
        }
        if (lastId != -1) {
            consumer.onEndList();
        }
        consumer.onEndMap();
    }

    public YTreeMapNode toYTreeMap(TableSchema schema) {
        YTreeBuilder builder = YTree.builder();
        writeTo(builder, schema);
        return builder.build().mapNode();
    }
}
