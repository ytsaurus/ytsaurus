package ru.yandex.yt.ytclient.proxy;

import com.google.protobuf.Message;
import tech.ytsaurus.client.SerializationResolver;
import tech.ytsaurus.client.rows.ConsumerSource;
import tech.ytsaurus.client.rows.MappedRowSerializer;
import tech.ytsaurus.client.rows.MappedRowsetDeserializer;
import tech.ytsaurus.client.rows.WireRowSerializer;
import tech.ytsaurus.client.rows.WireRowsetDeserializer;
import tech.ytsaurus.client.rows.YTreeDeserializer;
import tech.ytsaurus.core.rows.YTreeRowSerializer;
import tech.ytsaurus.core.rows.YTreeSerializer;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeNode;

import ru.yandex.inside.yt.kosher.impl.ytree.YTreeProtoUtils;
import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.YTreeObjectSerializer;
import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.YTreeObjectSerializerFactory;

public class YandexSerializationResolver implements SerializationResolver {
    private static final SerializationResolver INSTANCE = new YandexSerializationResolver();

    private YandexSerializationResolver() {
    }

    public static SerializationResolver getInstance() {
        return INSTANCE;
    }

    @Override
    public <T> YTreeRowSerializer<T> forClass(Class<T> clazz, TableSchema schema) {
        YTreeSerializer<T> serializer = YTreeObjectSerializerFactory.forClass(clazz, schema);
        if (!(serializer instanceof YTreeRowSerializer)) {
            throw new IllegalArgumentException("Unsupported class: " + clazz);
        }
        return (YTreeRowSerializer<T>) serializer;
    }

    @Override
    public <T> WireRowsetDeserializer<T> createWireRowDeserializer(YTreeSerializer<T> serializer) {
        if (serializer instanceof YTreeObjectSerializer) {
            return MappedRowsetDeserializer.forClass((YTreeObjectSerializer<T>) serializer);
        } else {
            return new YTreeDeserializer<>(serializer);
        }
    }

    @Override
    public <T> WireRowsetDeserializer<T> createWireRowDeserializer(TableSchema schema,
                                                                   YTreeSerializer<T> serializer,
                                                                   ConsumerSource<T> consumer) {
        if (serializer instanceof YTreeObjectSerializer) {
            return MappedRowsetDeserializer.forClass(schema, (YTreeObjectSerializer<T>) serializer, consumer);
        } else {
            YTreeDeserializer<T> deserializer = new YTreeDeserializer<>(serializer, consumer);
            deserializer.updateSchema(schema);
            return deserializer;
        }
    }

    @Override
    public <T> WireRowSerializer<T> createWireRowSerializer(YTreeSerializer<T> serializer) {
        return MappedRowSerializer.forClass(serializer);
    }

    @Override
    public <T> TableSchema asTableSchema(YTreeSerializer<T> serializer) {
        return MappedRowSerializer.asTableSchema(serializer.getFieldMap());
    }

    @Override
    public YTreeNode toTree(Object value) {
        if (value instanceof Message) {
            return YTreeProtoUtils.marshal((Message) value);
        } else if (value instanceof YTreeNode) {
            return (YTreeNode) value;
        } else {
            return YTree.builder().value(value).build();
        }
    }
}
