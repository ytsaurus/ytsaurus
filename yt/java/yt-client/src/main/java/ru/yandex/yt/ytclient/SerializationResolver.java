package ru.yandex.yt.ytclient;

import ru.yandex.inside.yt.kosher.impl.ytree.object.YTreeRowSerializer;
import ru.yandex.inside.yt.kosher.impl.ytree.object.YTreeSerializer;
import ru.yandex.inside.yt.kosher.ytree.YTreeNode;
import ru.yandex.yt.ytclient.object.ConsumerSource;
import ru.yandex.yt.ytclient.object.WireRowSerializer;
import ru.yandex.yt.ytclient.object.WireRowsetDeserializer;
import ru.yandex.yt.ytclient.tables.TableSchema;

public interface SerializationResolver {
    <T> YTreeRowSerializer<T> forClass(Class<T> clazz, TableSchema schema);

    <T> WireRowsetDeserializer<T> createWireRowDeserializer(YTreeSerializer<T> serializer);

    <T> WireRowsetDeserializer<T> createWireRowDeserializer(TableSchema schema,
                                                            YTreeSerializer<T> serializer,
                                                            ConsumerSource<T> consumer);

    <T> WireRowSerializer<T> createWireRowSerializer(YTreeSerializer<T> serializer);

    <T> TableSchema asTableSchema(YTreeSerializer<T> serializer);

    YTreeNode toTree(Object value);
}
