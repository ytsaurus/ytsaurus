package tech.ytsaurus.client;

import tech.ytsaurus.client.rows.ConsumerSource;
import tech.ytsaurus.client.rows.WireRowSerializer;
import tech.ytsaurus.client.rows.WireRowsetDeserializer;
import tech.ytsaurus.ysontree.YTreeNode;

import ru.yandex.inside.yt.kosher.impl.ytree.object.YTreeRowSerializer;
import ru.yandex.inside.yt.kosher.impl.ytree.object.YTreeSerializer;
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
