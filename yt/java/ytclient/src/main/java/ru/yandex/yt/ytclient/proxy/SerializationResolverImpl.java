package ru.yandex.yt.ytclient.proxy;

import ru.yandex.inside.yt.kosher.impl.ytree.object.YTreeRowSerializer;
import ru.yandex.inside.yt.kosher.impl.ytree.object.YTreeSerializer;
import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.YTreeObjectSerializer;
import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.YTreeObjectSerializerFactory;
import ru.yandex.yt.ytclient.SerializationResolver;
import ru.yandex.yt.ytclient.object.ConsumerSource;
import ru.yandex.yt.ytclient.object.MappedRowSerializer;
import ru.yandex.yt.ytclient.object.MappedRowsetDeserializer;
import ru.yandex.yt.ytclient.object.WireRowSerializer;
import ru.yandex.yt.ytclient.object.WireRowsetDeserializer;
import ru.yandex.yt.ytclient.object.YTreeDeserializer;
import ru.yandex.yt.ytclient.tables.TableSchema;

public class SerializationResolverImpl implements SerializationResolver {
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
}
