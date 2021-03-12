package ru.yandex.yt.ytclient.object;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import javax.annotation.Nonnull;

import ru.yandex.inside.yt.kosher.impl.ytree.object.YTreeObjectField;
import ru.yandex.inside.yt.kosher.impl.ytree.object.YTreeSerializer;
import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.AbstractYTreeDateSerializer;
import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.YTreeBytesToStringSerializer;
import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.YTreeNullSerializer;
import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.YTreeOptionSerializer;
import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.YTreeOptionalSerializer;
import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.simple.YTreeBooleanSerializer;
import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.simple.YTreeDoubleSerializer;
import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.simple.YTreeDurationSerializer;
import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.simple.YTreeEnumSerializer;
import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.simple.YTreeFloatSerializer;
import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.simple.YTreeInstantSerializer;
import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.simple.YTreeIntEnumSerializer;
import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.simple.YTreeIntegerSerializer;
import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.simple.YTreeJavaInstantSerializer;
import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.simple.YTreeLocalDateTimeSerializer;
import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.simple.YTreeLongSerializer;
import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.simple.YTreeOffsetDateTimeSerializer;
import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.simple.YTreeStringEnumSerializer;
import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.simple.YTreeStringSerializer;
import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.simple.YTreeUnsignedLongSerializer;
import ru.yandex.inside.yt.kosher.impl.ytree.serialization.YTreeBinarySerializer;
import ru.yandex.inside.yt.kosher.impl.ytree.serialization.YTreeStateSupport;
import ru.yandex.yson.ClosableYsonConsumer;
import ru.yandex.yson.YsonConsumer;
import ru.yandex.yt.rpcproxy.TRowsetDescriptor;
import ru.yandex.yt.ytclient.tables.ColumnSchema;
import ru.yandex.yt.ytclient.tables.ColumnSortOrder;
import ru.yandex.yt.ytclient.tables.ColumnValueType;
import ru.yandex.yt.ytclient.tables.TableSchema;

public class MappedRowSerializer<T> implements WireRowSerializer<T> {

    // Маленький размер буфера для кодирования заголовков и размеров
    private static final int BUFFER_SIZE = 64;
    private static final int OUTPUT_SIZE = 256;

    public static <T> MappedRowSerializer<T> forClass(YTreeSerializer<T> serializer) {
        return new MappedRowSerializer<>(serializer);
    }

    private final YTreeSerializer<T> objectSerializer;
    private final TableSchema tableSchema;
    private final YTreeConsumerProxy delegate;
    private final boolean supportState;

    private MappedRowSerializer(YTreeSerializer<T> objectSerializer) {
        this.objectSerializer = Objects.requireNonNull(objectSerializer);
        this.tableSchema = asTableSchema(objectSerializer.getFieldMap());
        this.delegate = new YTreeConsumerProxy(tableSchema);
        this.supportState = objectSerializer.getClazz().isAssignableTo(YTreeStateSupport.class);
    }

    @Override
    public TableSchema getSchema() {
        return tableSchema;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void serializeRow(T row, WireProtocolWriteable writeable, boolean keyFieldsOnly, int[] idMapping) {
        final T compareWith = !keyFieldsOnly && supportState
                ? ((YTreeStateSupport<? extends T>) row).getYTreeObjectState()
                : null;
        this.objectSerializer.serialize(row, delegate.wrap(writeable), false, keyFieldsOnly, compareWith);
        delegate.complete();
    }

    @Override
    public void updateSchema(TRowsetDescriptor schemaDelta) {
        // TODO: maybe update this.tableSchema here?

        delegate.updateSchema(schemaDelta);
    }

    public static TableSchema asTableSchema(Map<String, YTreeObjectField<?>> fieldMap) {
        final TableSchema.Builder builder = new TableSchema.Builder();
        asTableSchema(builder, fieldMap.values());
        return builder.build();
    }

    public static YTreeSerializer<?> unwrap(YTreeSerializer<?> serializer) {
        if (serializer instanceof YTreeOptionSerializer) {
            return unwrap(((YTreeOptionSerializer<?>) serializer).getDelegate());
        } else if (serializer instanceof YTreeOptionalSerializer) {
            return unwrap(((YTreeOptionalSerializer<?>) serializer).getDelegate());
        } else if (serializer instanceof YTreeNullSerializer) {
            return unwrap(((YTreeNullSerializer<?>) serializer).getDelegate());
        } else {
            return serializer;
        }
    }

    static ColumnValueType asType(YTreeSerializer<?> serializer) {
        if (serializer instanceof YTreeIntegerSerializer || serializer instanceof YTreeLongSerializer ||
                serializer instanceof YTreeIntEnumSerializer || serializer instanceof YTreeInstantSerializer ||
                serializer instanceof YTreeDurationSerializer || serializer instanceof YTreeJavaInstantSerializer ||
                serializer instanceof YTreeOffsetDateTimeSerializer) {
            return ColumnValueType.INT64;
        } else if (serializer instanceof YTreeDoubleSerializer || serializer instanceof YTreeFloatSerializer) {
            return ColumnValueType.DOUBLE;
        } else if (serializer instanceof YTreeUnsignedLongSerializer) {
            return ColumnValueType.UINT64;
        } else if (serializer instanceof YTreeBooleanSerializer) {
            return ColumnValueType.BOOLEAN;
        } else if (serializer instanceof YTreeStringEnumSerializer || serializer instanceof YTreeStringSerializer
                || serializer instanceof YTreeEnumSerializer || serializer instanceof AbstractYTreeDateSerializer
                || serializer instanceof YTreeLocalDateTimeSerializer
                || serializer instanceof YTreeBytesToStringSerializer) {
            return ColumnValueType.STRING;
        } else {
            return ColumnValueType.ANY;
        }
    }

    private static void asTableSchema(TableSchema.Builder builder, Collection<YTreeObjectField<?>> fields) {
        boolean hasKeys = false;

        for (YTreeObjectField<?> field : fields) {
            final YTreeSerializer<?> serializer = unwrap(field.serializer);
            if (field.isFlatten) {
                asTableSchema(builder, serializer.getFieldMap().values());
            } else {
                hasKeys |= field.isKeyField;

                builder.add(new ColumnSchema(field.key, asType(serializer),
                        field.isKeyField ? ColumnSortOrder.ASCENDING : null, null, null, null, null, field.isKeyField));
            }
        }

        builder.setUniqueKeys(hasKeys);
    }

    private static class YTreeConsumerProxy implements YsonConsumer {
        private final YTreeConsumerDirect direct;

        private ByteArrayOutputStream output;
        private ClosableYsonConsumer binarySerializer;
        private YsonConsumer current;
        private int level;

        private YTreeConsumerProxy(TableSchema tableSchema) {
            this.direct = new YTreeConsumerDirect(tableSchema);
        }

        YsonConsumer wrap(WireProtocolWriteable writeable) {
            direct.wrap(writeable);
            this.current = null;
            this.binarySerializer = null;
            this.level = 0;
            return this;
        }

        void updateSchema(TRowsetDescriptor schemaDelta) {
            direct.updateSchema(schemaDelta);
        }

        private void complete() {
            direct.complete();
        }

        private void registerBinarySerializer() {
            if (binarySerializer != null) {
                throw new IllegalStateException("Binary serializer must be empty at this state");
            }
            if (output == null) {
                output = new ByteArrayOutputStream(OUTPUT_SIZE);
            } else {
                output.reset();
            }
            binarySerializer = YTreeBinarySerializer.getSerializer(output, BUFFER_SIZE); // TODO: improve performance
            current = binarySerializer;
        }

        private void unregisterBinarySerializer() {
            current = direct;
            binarySerializer.close();
            direct.onBytesDirect(output.toByteArray());
            binarySerializer = null;
        }

        @Override
        public void onUnsignedInteger(long value) {
            current.onUnsignedInteger(value);
        }

        @Override
        public void onString(@Nonnull String value) {
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
        public void onKeyedItem(@Nonnull byte[] value, int offset, int length) {
            onKeyedItem(new String(value, offset, length, StandardCharsets.UTF_8));
        }

        @Override
        public void onKeyedItem(@Nonnull String key) {
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
        public void onString(@Nonnull byte[] bytes, int offset, int length) {
            current.onString(bytes, offset, length);
        }
    }

    private static class YTreeConsumerDirect implements YsonConsumer {

        // Сериализуем столбцы по одному, пока не наткнемся на любое поле, кроме примитивного
        // Т.е. это будет любой
        private final Map<String, ColumnWithIndex> schema;
        private WireProtocolWriteable writeable;

        private ColumnWithIndex currentColumn;
        private int columnCount;

        private YTreeConsumerDirect(TableSchema tableSchema) {
            this.schema = new HashMap<>(tableSchema.getColumnsCount());

            for (int i = 0; i < tableSchema.getColumnsCount(); i++) {
                final ColumnSchema columnSchema = tableSchema.getColumnSchema(i);
                if (columnSchema != null) {
                    schema.put(columnSchema.getName(), new ColumnWithIndex(i, columnSchema.getType()));
                }
            }
        }

        void updateSchema(TRowsetDescriptor schemaDelta) {
            for (TRowsetDescriptor.TNameTableEntry entry : schemaDelta.getNameTableEntriesList()) {
                if (!schema.containsKey(entry.getName())) {
                    int index = schema.size();
                    schema.put(entry.getName(), new ColumnWithIndex(index, ColumnValueType.fromValue(entry.getType())));
                }
            }
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
        public void onUnsignedInteger(long value) {
            if (currentColumn != null) {
                this.onInteger(value);
            }
        }

        @Override
        public void onString(@Nonnull String value) {
            if (currentColumn != null) {
                this.onBytesDirect(value.getBytes(StandardCharsets.UTF_8));
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
        public void onKeyedItem(@Nonnull byte[] value, int offset, int length) {
            onKeyedItem(new String(value, offset, length, StandardCharsets.UTF_8));
        }

        @Override
        public void onKeyedItem(@Nonnull String key) {
            this.currentColumn = this.schema.get(key);

            if (this.schema.isEmpty()) {
                throw new IllegalStateException();
            }

            if (this.currentColumn != null) {
                this.columnCount++;
            }
        }

        @Override
        public void onEntity() {
            if (currentColumn != null) {
                // write empty value
                writeable.writeValueHeader(currentColumn.columnId, ColumnValueType.NULL, false, 0);
            }
        }

        @Override
        public void onInteger(long value) {
            if (currentColumn != null) {
                writeable.writeValueHeader(currentColumn.columnId, currentColumn.columnType, false, 0);
                writeable.onInteger(value);
            }
        }

        @Override
        public void onBoolean(boolean value) {
            if (currentColumn != null) {
                writeable.writeValueHeader(currentColumn.columnId, currentColumn.columnType, false, 0);
                writeable.onBoolean(value);
            }
        }

        @Override
        public void onDouble(double value) {
            if (currentColumn != null) {
                writeable.writeValueHeader(currentColumn.columnId, currentColumn.columnType, false, 0);
                writeable.onDouble(value);
            }
        }

        @Override
        public void onString(@Nonnull byte[] bytes, int offset, int length) {
            if (currentColumn != null) {
                if (currentColumn.columnType == ColumnValueType.STRING) {
                    this.onBytesDirect(bytes);
                } else {
                    if (currentColumn.columnType != ColumnValueType.ANY) {
                        throw new IllegalStateException();
                    }

                    // Это может быть только ANY тип и в этом случае мы должны корректно сериализовать массив байтов
                    final ByteArrayOutputStream output = new ByteArrayOutputStream(bytes.length + 1 + 4);

                    try (ClosableYsonConsumer binarySerializer = YTreeBinarySerializer.getSerializer(output)) {
                        binarySerializer.onString(bytes, offset, length); // TODO: improve performance
                    }
                    this.onBytesDirect(output.toByteArray());
                }
            }
        }

        void onBytesDirect(byte[] bytes) {
            if (currentColumn != null) {
                writeable.writeValueHeader(currentColumn.columnId, currentColumn.columnType, false, bytes.length);
                writeable.onBytes(bytes);
            }
        }
    }

    static class ColumnWithIndex {
        private final int columnId;
        private final ColumnValueType columnType;

        ColumnWithIndex(int columnId, ColumnValueType columnType) {
            this.columnId = columnId;
            this.columnType = columnType;
        }
    }
}
