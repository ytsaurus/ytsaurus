package ru.yandex.yt.ytclient.object;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;

import ru.yandex.inside.yt.kosher.impl.ytree.object.YTreeObjectField;
import ru.yandex.inside.yt.kosher.impl.ytree.object.YTreeSerializer;
import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.YTreeNullSerializer;
import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.YTreeObjectSerializer;
import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.YTreeOptionSerializer;
import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.simple.YTreeBooleanSerializer;
import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.simple.YTreeDoubleSerializer;
import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.simple.YTreeFloatSerializer;
import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.simple.YTreeInstantSerializer;
import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.simple.YTreeIntEnumSerializer;
import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.simple.YTreeIntegerSerializer;
import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.simple.YTreeLongSerializer;
import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.simple.YTreeStringEnumSerializer;
import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.simple.YTreeStringSerializer;
import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.simple.YTreeUnsignedLongSerializer;
import ru.yandex.inside.yt.kosher.impl.ytree.serialization.YTreeBinarySerializer;
import ru.yandex.inside.yt.kosher.impl.ytree.serialization.YTreeCloseableConsumer;
import ru.yandex.inside.yt.kosher.impl.ytree.serialization.YTreeConsumer;
import ru.yandex.misc.lang.number.UnsignedLong;
import ru.yandex.yt.ytclient.tables.ColumnSchema;
import ru.yandex.yt.ytclient.tables.ColumnSortOrder;
import ru.yandex.yt.ytclient.tables.ColumnValueType;
import ru.yandex.yt.ytclient.tables.TableSchema;

public class MappedRowSerializer<T> implements WireRowSerializer<T> {

    public static <T> MappedRowSerializer<T> forClass(YTreeObjectSerializer<T> serializer) {
        return new MappedRowSerializer<>(serializer);
    }

    private final YTreeObjectSerializer<T> objectSerializer;
    private final TableSchema tableSchema;
    private final YTreeConsumerProxy delegate;

    private MappedRowSerializer(YTreeObjectSerializer<T> objectSerializer) {
        this.objectSerializer = Objects.requireNonNull(objectSerializer);
        this.tableSchema = asTableSchema(objectSerializer.getFieldMap());
        this.delegate = new YTreeConsumerProxy(tableSchema);
    }

    @Override
    public TableSchema getSchema() {
        return tableSchema;
    }

    @Override
    public void serializeRow(T row, WireProtocolWriteable writeable) {
        this.objectSerializer.serialize(row, delegate.wrap(writeable), false);
        delegate.complete();
    }

    public static TableSchema asTableSchema(Map<String, YTreeObjectField<?>> fieldMap) {
        final TableSchema.Builder builder = new TableSchema.Builder();
        asTableSchema(builder, fieldMap.values());
        return builder.build();
    }

    private static void asTableSchema(TableSchema.Builder builder, Collection<YTreeObjectField<?>> fields) {
        for (YTreeObjectField<?> field : fields) {
            final YTreeSerializer<?> serializer = unwrap(field.serializer);
            if (field.isFlatten) {
                asTableSchema(builder, ((YTreeObjectSerializer<?>) serializer).getFieldMap().values());
            } else {
                builder.add(new ColumnSchema(field.key, asType(serializer),
                        field.isKeyField ? ColumnSortOrder.ASCENDING : null, null, null, null, null, field.isKeyField));
            }
        }
    }

    static YTreeSerializer<?> unwrap(YTreeSerializer serializer) {
        if (serializer instanceof YTreeOptionSerializer) {
            return unwrap(((YTreeOptionSerializer<?>) serializer).getDelegate());
        } else if (serializer instanceof YTreeNullSerializer) {
            return unwrap(((YTreeNullSerializer<?>) serializer).getDelegate());
        } else {
            return serializer;
        }
    }

    static ColumnValueType asType(YTreeSerializer<?> serializer) {
        if (serializer instanceof YTreeIntegerSerializer || serializer instanceof YTreeLongSerializer
                || serializer instanceof YTreeIntEnumSerializer || serializer instanceof YTreeInstantSerializer)
        {
            return ColumnValueType.INT64;
        } else if (serializer instanceof YTreeDoubleSerializer || serializer instanceof YTreeFloatSerializer) {
            return ColumnValueType.DOUBLE;
        } else if (serializer instanceof YTreeUnsignedLongSerializer) {
            return ColumnValueType.UINT64;
        } else if (serializer instanceof YTreeBooleanSerializer) {
            return ColumnValueType.BOOLEAN;
        } else if (serializer instanceof YTreeStringEnumSerializer || serializer instanceof YTreeStringSerializer) {
            return ColumnValueType.STRING;
        } else {
            return ColumnValueType.ANY;
        }
    }

    private static class YTreeConsumerProxy implements YTreeConsumer {
        private final YTreeConsumerDirect direct;

        private ByteArrayOutputStream output;
        private YTreeCloseableConsumer binarySerializer;
        private YTreeConsumer current;
        private int level;

        private YTreeConsumerProxy(TableSchema tableSchema) {
            this.direct = new YTreeConsumerDirect(tableSchema);
        }

        YTreeConsumer wrap(WireProtocolWriteable writeable) {
            direct.wrap(writeable);
            this.current = null;
            this.binarySerializer = null;
            this.level = 0;
            return this;
        }

        private void complete() {
            direct.complete();
        }

        private void registerBinarySerializer() {
            if (binarySerializer != null) {
                throw new IllegalStateException("Binary serializer must be empty at this state");
            }
            output = new ByteArrayOutputStream();
            binarySerializer = YTreeBinarySerializer.getSerializer(output); // TODO: improve performance
            current = binarySerializer;
        }

        private void unregisterBinarySerializer() {
            current = direct;
            binarySerializer.close();
            current.onBytes(output.toByteArray());
            output = null;
            binarySerializer = null;
        }

        @Override
        public void onUnsignedInteger(UnsignedLong value) {
            current.onUnsignedInteger(value);
        }

        @Override
        public void onString(String value) {
            current.onString(value);
        }

        @Override
        public void onListItem() {
            current.onListItem();
        }

        @Override
        public void onBeginList() {
            if (level == 1) {
                this.registerBinarySerializer();
            }
            current.onBeginList();
            level++;
        }

        @Override
        public void onEndList() {
            level--;
            current.onEndList();
            if (level == 1) {
                this.unregisterBinarySerializer();
            }
        }

        @Override
        public void onBeginAttributes() {
            current.onBeginAttributes();
        }

        @Override
        public void onEndAttributes() {
            current.onEndAttributes();
        }

        @Override
        public void onBeginMap() {
            if (level == 0) {
                current = direct;
            } else if (level == 1) {
                registerBinarySerializer();
                current.onBeginMap();
            } else if (level > 1) {
                current.onBeginMap();
            }
            level++;
        }

        @Override
        public void onEndMap() {
            level--;
            if (level == 1) {
                current.onEndMap();
                unregisterBinarySerializer();
            } else if (level > 1) {
                current.onEndMap();
            }
        }

        @Override
        public void onKeyedItem(String key) {
            current.onKeyedItem(key);
        }

        @Override
        public void onEntity() {
            current.onEntity();
        }

        @Override
        public void onInteger(long value) {
            current.onInteger(value);
        }

        @Override
        public void onBoolean(boolean value) {
            current.onBoolean(value);
        }

        @Override
        public void onDouble(double value) {
            current.onDouble(value);
        }

        @Override
        public void onBytes(byte[] bytes) {
            current.onBytes(bytes);
        }
    }

    private static class YTreeConsumerDirect implements YTreeConsumer {

        // Сериализуем столбцы по одному, пока не наткнемся на любое поле, кроме примитивного
        // Т.е. это будет любой
        private final TableSchema schema;
        private WireProtocolWriteable writeable;

        private int columnId;
        private ColumnValueType columnType;
        private int columnCount;

        private YTreeConsumerDirect(TableSchema schema) {
            this.schema = schema;
        }

        void complete() {
            this.writeable.overwriteValueCount(this.columnCount);
        }

        void wrap(WireProtocolWriteable writeable) {
            // Мы еще не знаем, сколько полей нам придется записать
            this.writeable = writeable;
            this.columnCount = 0;
            writeable.writeValueCount(0);
        }

        @Override
        public void onUnsignedInteger(UnsignedLong value) {
            if (columnId != -1) {
                this.onInteger(value.longValue());
            }
        }

        @Override
        public void onString(String value) {
            if (columnId != -1) {
                this.onBytes(value.getBytes(StandardCharsets.UTF_8));
            }
        }

        @Override
        public void onListItem() {
            throw new IllegalStateException("Unsupported operation");
        }

        @Override
        public void onBeginList() {
            throw new IllegalStateException("Unsupported operation");
        }

        @Override
        public void onEndList() {
            throw new IllegalStateException("Unsupported operation");
        }

        @Override
        public void onBeginAttributes() {
            throw new IllegalStateException("Unsupported operation");
        }

        @Override
        public void onEndAttributes() {
            throw new IllegalStateException("Unsupported operation");
        }

        @Override
        public void onBeginMap() {
            throw new IllegalStateException("Unsupported operation");
        }

        @Override
        public void onEndMap() {
            throw new IllegalStateException("Unsupported operation");
        }

        @Override
        public void onKeyedItem(String key) {
            this.columnId = this.schema.findColumn(key);
            if (columnId == -1) {
                return; // ---
            }
            this.columnType = this.schema.getColumnType(columnId);
            this.columnCount++;
        }

        @Override
        public void onEntity() {
            if (columnId != -1) {
                // write empty value
                writeable.writeValueHeader(columnId, ColumnValueType.NULL, false, 0);
            }
        }

        @Override
        public void onInteger(long value) {
            if (columnId != -1) {
                writeable.writeValueHeader(columnId, columnType, false, 0);
                writeable.onInteger(value);
            }
        }

        @Override
        public void onBoolean(boolean value) {
            if (columnId != -1) {
                writeable.writeValueHeader(columnId, columnType, false, 0);
                writeable.onBoolean(value);
            }
        }

        @Override
        public void onDouble(double value) {
            if (columnId != -1) {
                writeable.writeValueHeader(columnId, columnType, false, 0);
                writeable.onDouble(value);
            }
        }

        @Override
        public void onBytes(byte[] bytes) {
            if (columnId != -1) {
                writeable.writeValueHeader(columnId, columnType, false, bytes.length);
                writeable.onBytes(bytes);
            }
        }
    }
}
