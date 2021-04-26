package ru.yandex.yt.ytclient.object;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import javax.annotation.Nonnull;

import ru.yandex.bolts.collection.Cf;
import ru.yandex.bolts.collection.MapF;
import ru.yandex.inside.yt.kosher.impl.ytree.YTreeBooleanNodeImpl;
import ru.yandex.inside.yt.kosher.impl.ytree.YTreeDoubleNodeImpl;
import ru.yandex.inside.yt.kosher.impl.ytree.YTreeEntityNodeImpl;
import ru.yandex.inside.yt.kosher.impl.ytree.YTreeIntegerNodeImpl;
import ru.yandex.inside.yt.kosher.impl.ytree.YTreeStringNodeImpl;
import ru.yandex.inside.yt.kosher.impl.ytree.object.YTreeObjectField;
import ru.yandex.inside.yt.kosher.impl.ytree.object.YTreeSerializer;
import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.YTreeObjectSerializer;
import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.simple.YTreeStringSerializer;
import ru.yandex.inside.yt.kosher.impl.ytree.serialization.YTreeBinarySerializer;
import ru.yandex.inside.yt.kosher.ytree.YTreeNode;
import ru.yandex.yt.ytclient.tables.ColumnSchema;
import ru.yandex.yt.ytclient.tables.ColumnValueType;
import ru.yandex.yt.ytclient.tables.TableSchema;
import ru.yandex.yt.ytclient.wire.WireColumnSchema;
import ru.yandex.yt.ytclient.wire.WireProtocolReader;

public class MappedRowsetDeserializer<T> implements WireRowsetDeserializer<T>, WireValueDeserializer<Void>,
        WireVersionedRowsetDeserializer<T>, WireSchemafulRowsetDeserializer<T> {

    // Уменьшаем размер буфера - ожидаем, что объекты не очень большие
    private static final int BUFFER_SIZE = 512;

    private final MapF<String, YTreeNode> attributes = Cf.map();
    private final YTreeBooleanNodeImpl booleanNode = new YTreeBooleanNodeImpl(false, attributes);
    private final YTreeDoubleNodeImpl doubleNode = new YTreeDoubleNodeImpl(0, attributes);
    private final YTreeEntityNodeImpl entityNode = new YTreeEntityNodeImpl(attributes);
    private final YTreeIntegerNodeImpl integerNode = new YTreeIntegerNodeImpl(true, 0, attributes);
    private final YTreeStringNodeImpl stringNode = new YTreeStringNodeImpl((byte[]) null, attributes);

    private final YTreeObjectSerializer<T> objectSerializer;
    private ObjectFieldWrapper[] schemaFields;

    private final ConsumerSource<T> consumer;
    private TableSchema schema;
    private List<WireColumnSchema> columnSchema;

    private final SerializerConfiguration<T> configuration;

    private final int flattenStackSize;

    // Обноавляются при обработке новой строки
    private Object[] flattenStack;
    private T instance;

    // Обновляются при обработке нового поля
    private FlattenFieldWrapper flattenField;
    private Object flattenInstance;
    private ColumnValueType type;
    private ObjectFieldWrapper field;
    private YTreeNode node;

    private byte[] cache;

    private MappedRowsetDeserializer(TableSchema schema, SerializerConfiguration<T> configuration,
                                     ConsumerSource<T> consumer) {
        this.configuration = configuration;

        parseSchema(Objects.requireNonNull(schema));

        this.objectSerializer = Objects.requireNonNull(configuration.objectSerializer);
        this.consumer = Objects.requireNonNull(consumer);

        this.flattenStackSize = configuration.flattenWrappers.size();
    }

    public static <T> MappedRowsetDeserializer<T> forClass(YTreeObjectSerializer<T> serializer) {
        final TableSchema schema = MappedRowSerializer.asTableSchema(serializer.getFieldMap());
        return MappedRowsetDeserializer.forClass(schema, serializer, (unused) -> { });
    }

    public static <T> MappedRowsetDeserializer<T> forClass(TableSchema schema,
                                                           YTreeObjectSerializer<T> objectSerializer,
                                                           ConsumerSource<T> consumer) {
        return new MappedRowsetDeserializer<>(schema, new SerializerConfiguration<>(objectSerializer), consumer);
    }

    private void parseSchema(TableSchema schema) {
        this.columnSchema = WireProtocolReader.makeSchemaData(schema);

        final List<ColumnSchema> columns = schema.getColumns();
        this.schemaFields = new ObjectFieldWrapper[columns.size()];

        for (int i = 0; i < columns.size(); i++) {
            schemaFields[i] = configuration.fields.get(columns.get(i).getName());
        }

        this.schema = schema;
    }

    @Override
    public void updateSchema(@Nonnull TableSchema schema) {
        if (!this.schema.equals(schema)) {
            parseSchema(schema);
        }
    }

    private static class SerializerConfiguration<T> {
        private final YTreeObjectSerializer<T> objectSerializer;
        private final List<FlattenFieldWrapper> flattenWrappers = new ArrayList<>();
        private final Map<String, ObjectFieldWrapper> fields = new LinkedHashMap<>();

        SerializerConfiguration(YTreeObjectSerializer<T> objectSerializer) {
            this.objectSerializer = Objects.requireNonNull(objectSerializer);
            this.collectFields(objectSerializer.getFieldMap().values(), null);
        }

        private void collectFields(Collection<YTreeObjectField<?>> fields, FlattenFieldWrapper parent) {
            for (YTreeObjectField<?> field : fields) {
                final boolean isFlatten = field.isFlatten;
                final YTreeSerializer<?> serializer = MappedRowSerializer.unwrap(field.serializer);
                if (isFlatten) {
                    final FlattenFieldWrapper wrapper = new FlattenFieldWrapper(parent, field, flattenWrappers.size());
                    flattenWrappers.add(wrapper);
                    collectFields(serializer.getFieldMap().values(), wrapper);
                } else {
                    if (this.fields.put(field.key, new ObjectFieldWrapper(field, parent)) != null) {
                        throw new IllegalStateException(
                                "Invalid flatten field configuration. Found duplicate: " + field.key);
                    }
                }
            }
        }
    }

    @Override
    public void setRowCount(int rowCount) {
        consumer.setRowCount(rowCount);
    }

    @Override
    public List<WireColumnSchema> getColumnSchema() {
        return this.columnSchema;
    }

    @Nonnull
    @Override
    public WireValueDeserializer<Void> onNewRow(int columnCount) {
        this.instance = objectSerializer.newInstance();
        this.flattenStack = null;
        return this;
    }

    @Override
    public void onWriteTimestamps(List<Long> timestamps) {
        // TODO: support applying versioned mapping?
    }

    @Override
    public void onDeleteTimestamps(List<Long> timestamps) {
        // TODO: support applying versioned mapping?
    }

    @Override
    public List<WireColumnSchema> getKeyColumnSchema() {
        return this.columnSchema;
    }

    @Override
    public WireValueDeserializer<?> keys(int keyCount) {
        this.onNewRow(keyCount);
        return this; // Ключи и значения пишем в один и тот же объект
    }

    @Override
    public WireValueDeserializer<?> values(int valueCount) {
        return this; // Ключи и значения пишем в один и тот же объект
    }

    @Override
    @Nonnull
    public T onCompleteRow() {
        consumer.accept(instance);
        return instance;
    }

    @Override
    public T onNullRow() {
        consumer.accept(null);
        return null;
    }

    @Override
    public void setId(int id) {
        this.field = null;
        this.flattenField = null;
        this.flattenInstance = null;

        if (id >= 0 && id < schemaFields.length) {
            this.field = schemaFields[id];
            if (this.field == null) {
                return; // ---
            }
            this.prepareFlatten();
        }
    }

    @Override
    public void setType(ColumnValueType type) {
        this.type = type; // TODO: check type?
        this.node = null;
    }

    @Override
    public void setAggregate(boolean aggregate) {
        // TODO: handle?
    }

    @Override
    public void setTimestamp(long timestamp) {
        // TODO: handle?
    }

    @Override
    public void onEntity() {
        this.node = entityNode;
    }

    @Override
    public void onInteger(long value) {
        switch (type) {
            case INT64:
                integerNode.setLong(value);
                this.node = integerNode;
                break;
            case UINT64:
                integerNode.setUnsignedLong(value);
                this.node = integerNode;
                break;
            default:
                // Мы не должны сюда попасть, но все же
                this.onEntity();
        }
    }

    @Override
    public void onBoolean(boolean value) {
        booleanNode.setValue(value);
        this.node = booleanNode;
    }

    @Override
    public void onDouble(double value) {
        doubleNode.setValue(value);
        this.node = doubleNode;
    }

    @Override
    public void onBytes(byte[] bytes) {
        switch (type) {
            case STRING:
                stringNode.setBytes(bytes);
                this.node = stringNode;
                break;
            case ANY:
                if (bytes == null || bytes.length == 0) {
                    this.onEntity();
                } else if (field != null && field.stringNode) {
                    stringNode.setBytes(bytes);
                    this.node = stringNode;
                } else {
                    if (cache == null) {
                        cache = new byte[BUFFER_SIZE];
                    } else {
                        Arrays.fill(cache, (byte) 0);
                    }
                    this.node = YTreeBinarySerializer
                            .deserialize(new ByteArrayInputStream(bytes), cache); // TODO: improve performance
                }
                break;
            default:
                this.onEntity();
        }
    }


    @SuppressWarnings("unchecked")
    @Override
    public Void build() {
        if (field != null) {
            try {
                if (flattenField != null) {
                    Objects.requireNonNull(this.flattenInstance, "Flatten instance cannot be null at this point");
                    flattenField.serializer.deserializeIntoObject(this.flattenInstance, this.field.objectField,
                            this.node);
                } else {
                    Objects.requireNonNull(this.instance, "Instance cannot be null at this point");
                    objectSerializer.deserializeIntoObject(this.instance, this.field.objectField, this.node);
                }
            } catch (RuntimeException e) {
                throw new RuntimeException(
                        "Unable to deserialize field " + this.field.objectField.key + " as " + type,
                        e);
            }
        }
        return null;
    }

    private void prepareFlatten() {
        this.flattenField = this.field.parent;
        if (this.flattenField == null) {
            return; // ---

        }
        if (this.flattenStack == null) {
            this.flattenStack = new Object[this.flattenStackSize];
        }
        this.flattenInstance = restoreFlattenStack(flattenField);
    }

    // Метод раскручивает стек Flatten объектов, инициализируя каждый объект единожды
    private Object restoreFlattenStack(FlattenFieldWrapper wrapper) {
        Object object = this.flattenStack[wrapper.index];
        if (object != null) {
            return object;
        }
        object = wrapper.serializer.newInstance();
        this.flattenStack[wrapper.index] = object;

        final Object parentObject;
        if (wrapper.parent == null) { // Our parent if current instance
            Objects.requireNonNull(this.instance, "Instance cannot be null at this point");
            parentObject = this.instance;
        } else {
            parentObject = restoreFlattenStack(wrapper.parent);
        }
        wrapper.objectField.field.set(parentObject, object);
        return object;
    }


    private static class FlattenFieldWrapper {
        private final FlattenFieldWrapper parent;
        private final YTreeObjectField<?> objectField;
        private final YTreeObjectSerializer serializer;
        private final int index;

        private FlattenFieldWrapper(FlattenFieldWrapper parent, YTreeObjectField<?> objectField, int index) {
            this.parent = parent;
            this.objectField = Objects.requireNonNull(objectField);
            this.serializer = (YTreeObjectSerializer<?>) MappedRowSerializer.unwrap(objectField.serializer);
            this.index = index;
        }
    }

    private static class ObjectFieldWrapper {
        private final YTreeObjectField<?> objectField;
        private final boolean stringNode;
        private final FlattenFieldWrapper parent;

        ObjectFieldWrapper(YTreeObjectField<?> objectField, FlattenFieldWrapper parent) {
            this.objectField = Objects.requireNonNull(objectField);
            final YTreeSerializer<?> serializer = MappedRowSerializer.unwrap(objectField.serializer);
            this.stringNode = serializer instanceof YTreeStringSerializer;
            this.parent = parent;
        }
    }
}
