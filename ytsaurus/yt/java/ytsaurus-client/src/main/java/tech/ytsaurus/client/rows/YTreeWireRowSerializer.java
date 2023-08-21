package tech.ytsaurus.client.rows;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import tech.ytsaurus.core.rows.YTreeRowSerializer;
import tech.ytsaurus.core.rows.YTreeSerializer;
import tech.ytsaurus.core.tables.ColumnSchema;
import tech.ytsaurus.core.tables.ColumnValueType;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.rpcproxy.TRowsetDescriptor;
import tech.ytsaurus.typeinfo.TiType;
import tech.ytsaurus.yson.ClosableYsonConsumer;
import tech.ytsaurus.yson.YsonConsumer;
import tech.ytsaurus.ysontree.YTreeBinarySerializer;

public class YTreeWireRowSerializer<T> implements WireRowSerializer<T> {
    // Маленький размер буфера для кодирования заголовков и размеров
    private static final int BUFFER_SIZE = 64;
    private static final int OUTPUT_SIZE = 256;

    protected final YTreeRowSerializer<T> objectSerializer;
    protected TableSchema tableSchema;
    protected YTreeConsumerProxy delegate;

    protected YTreeWireRowSerializer(YTreeRowSerializer<T> objectSerializer) {
        this.objectSerializer = Objects.requireNonNull(objectSerializer);
        this.tableSchema = TableSchema.builder().build();
        this.delegate = new YTreeConsumerProxy(tableSchema);
    }

    public static <T> YTreeWireRowSerializer<T> forClass(YTreeSerializer<T> serializer) {
        if (serializer instanceof YTreeRowSerializer) {
            return new YTreeWireRowSerializer<>((YTreeRowSerializer<T>) serializer);
        }
        throw new RuntimeException("Expected YTreeRowSerializer in YTreeWireRowSerializer.forClass()");
    }

    @Override
    public TableSchema getSchema() {
        return tableSchema;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void serializeRow(T row, WireProtocolWriteable writeable, boolean keyFieldsOnly, boolean aggregate,
                             int[] idMapping) {
        this.objectSerializer.serializeRow(row, delegate.wrap(writeable, aggregate), keyFieldsOnly, null);
        delegate.complete();
    }

    @Override
    public void updateSchema(TRowsetDescriptor schemaDelta) {
        TableSchema.Builder builder = this.tableSchema.toBuilder();
        for (TRowsetDescriptor.TNameTableEntry entry : schemaDelta.getNameTableEntriesList()) {
            if (tableSchema.findColumn(entry.getName()) == -1) {
                builder.add(new ColumnSchema(entry.getName(), ColumnValueType.fromValue(entry.getType())));
            }
        }
        this.tableSchema = builder.build();
        delegate.updateSchema(schemaDelta);
    }

    static TiType asType(YTreeSerializer<?> serializer) {
        TiType type = serializer.getColumnValueType();
        if (type.isNullable()) {
            return type;
        } else {
            return TiType.optional(type);
        }
    }

    protected static class YTreeConsumerProxy implements YsonConsumer {
        private final YTreeConsumerDirect direct;

        private ByteArrayOutputStream output;
        private ClosableYsonConsumer binarySerializer;
        private YsonConsumer current;
        private int level;

        protected YTreeConsumerProxy(TableSchema tableSchema) {
            this.direct = new YTreeConsumerDirect(tableSchema);
        }

        YsonConsumer wrap(WireProtocolWriteable writeable, boolean aggregate) {
            direct.wrap(writeable, aggregate);
            this.current = null;
            this.binarySerializer = null;
            this.level = 0;
            return this;
        }

        void updateSchema(TRowsetDescriptor schemaDelta) {
            direct.updateSchema(schemaDelta);
        }

        void complete() {
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
        private boolean aggregate = false;

        private YTreeConsumerDirect(TableSchema tableSchema) {
            this.schema = new HashMap<>(tableSchema.getColumnsCount());

            for (int i = 0; i < tableSchema.getColumnsCount(); i++) {
                final ColumnSchema columnSchema = tableSchema.getColumnSchema(i);
                if (columnSchema != null) {
                    schema.put(columnSchema.getName(), new ColumnWithIndex(i, columnSchema.getType(),
                            columnSchema.getAggregate()));
                }
            }
        }

        void updateSchema(TRowsetDescriptor schemaDelta) {
            for (TRowsetDescriptor.TNameTableEntry entry : schemaDelta.getNameTableEntriesList()) {
                if (!schema.containsKey(entry.getName())) {
                    int index = schema.size();
                    schema.put(entry.getName(), new ColumnWithIndex(index, ColumnValueType.fromValue(entry.getType()),
                            null));
                }
            }
        }

        void complete() {
            this.writeable.overwriteValueCount(this.columnCount);
        }

        void wrap(WireProtocolWriteable writeable, boolean aggregate) {
            // Мы еще не знаем, сколько полей нам придется записать
            this.writeable = writeable;
            this.columnCount = 0;
            this.aggregate = aggregate;
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
                writeable.writeValueHeader(currentColumn.columnId, ColumnValueType.NULL,
                        aggregate && currentColumn.aggregate != null, 0);
            }
        }

        @Override
        public void onInteger(long value) {
            if (currentColumn != null) {
                writeable.writeValueHeader(currentColumn.columnId, currentColumn.columnType,
                        aggregate && currentColumn.aggregate != null, 0);
                writeable.onInteger(value);
            }
        }

        @Override
        public void onBoolean(boolean value) {
            if (currentColumn != null) {
                writeable.writeValueHeader(currentColumn.columnId, currentColumn.columnType,
                        aggregate && currentColumn.aggregate != null, 0);
                writeable.onBoolean(value);
            }
        }

        @Override
        public void onDouble(double value) {
            if (currentColumn != null) {
                writeable.writeValueHeader(currentColumn.columnId, currentColumn.columnType,
                        aggregate && currentColumn.aggregate != null, 0);
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
                writeable.writeValueHeader(currentColumn.columnId, currentColumn.columnType,
                        aggregate && currentColumn.aggregate != null, bytes.length);
                writeable.onBytes(bytes);
            }
        }
    }

    static class ColumnWithIndex {
        private final int columnId;
        private final ColumnValueType columnType;
        @Nullable
        private final String aggregate;

        ColumnWithIndex(int columnId, ColumnValueType columnType, @Nullable String aggregate) {
            this.columnId = columnId;
            this.columnType = columnType;
            this.aggregate = aggregate;
        }
    }
}
