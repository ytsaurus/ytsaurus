package tech.ytsaurus.client;

import tech.ytsaurus.client.rows.ConsumerSource;
import tech.ytsaurus.client.rows.WireRowSerializer;
import tech.ytsaurus.client.rows.WireRowsetDeserializer;
import tech.ytsaurus.client.rows.YTreeDeserializer;
import tech.ytsaurus.client.rows.YTreeWireRowSerializer;
import tech.ytsaurus.core.rows.YTreeMapNodeSerializer;
import tech.ytsaurus.core.rows.YTreeRowSerializer;
import tech.ytsaurus.core.rows.YTreeSerializer;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeMapNode;
import tech.ytsaurus.ysontree.YTreeNode;


public class DefaultSerializationResolver implements SerializationResolver {
    private static final SerializationResolver INSTANCE = new DefaultSerializationResolver();

    private DefaultSerializationResolver() {
    }

    public static SerializationResolver getInstance() {
        return INSTANCE;
    }

    @Override
    public <T> YTreeRowSerializer<T> forClass(Class<T> clazz, TableSchema schema) {
        if (clazz.equals(YTreeMapNode.class)) {
            return (YTreeRowSerializer<T>) new YTreeMapNodeSerializer();
        } else {
            throw new IllegalArgumentException("Unsupported class: " + clazz);
        }
    }

    @Override
    public <T> WireRowsetDeserializer<T> createWireRowDeserializer(YTreeSerializer<T> serializer) {
        return new YTreeDeserializer<>(serializer);
    }

    @Override
    public <T> WireRowsetDeserializer<T> createWireRowDeserializer(TableSchema schema,
                                                                   YTreeSerializer<T> serializer,
                                                                   ConsumerSource<T> consumer) {
        YTreeDeserializer<T> deserializer = new YTreeDeserializer<>(serializer, consumer);
        deserializer.updateSchema(schema);
        return deserializer;
    }

    @Override
    public <T> WireRowSerializer<T> createWireRowSerializer(YTreeSerializer<T> serializer) {
        return YTreeWireRowSerializer.forClass(serializer);
    }

    @Override
    public <T> TableSchema asTableSchema(YTreeSerializer<T> serializer) {
        return TableSchema.builder().build();
    }

    @Override
    public YTreeNode toTree(Object value) {
        if (value instanceof YTreeNode) {
            return (YTreeNode) value;
        } else {
            return YTree.builder().value(value).build();
        }
    }
}
