package ru.yandex.yt.ytclient;

import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeMapNode;
import tech.ytsaurus.ysontree.YTreeNode;

import ru.yandex.inside.yt.kosher.impl.ytree.object.YTreeRowSerializer;
import ru.yandex.inside.yt.kosher.impl.ytree.object.YTreeSerializer;
import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.YTreeMapNodeSerializer;
import ru.yandex.yt.ytclient.object.ConsumerSource;
import ru.yandex.yt.ytclient.object.WireRowSerializer;
import ru.yandex.yt.ytclient.object.WireRowsetDeserializer;
import ru.yandex.yt.ytclient.object.YTreeDeserializer;
import ru.yandex.yt.ytclient.object.YTreeWireRowSerializer;
import ru.yandex.yt.ytclient.tables.TableSchema;

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
            return (YTreeRowSerializer<T>) new YTreeMapNodeSerializer((Class<YTreeMapNode>) clazz);
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
        return TableSchema.newBuilder().build();
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
