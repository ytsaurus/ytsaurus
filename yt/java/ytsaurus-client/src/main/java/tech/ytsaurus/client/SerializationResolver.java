package tech.ytsaurus.client;

import tech.ytsaurus.client.rows.ConsumerSource;
import tech.ytsaurus.client.rows.WireRowSerializer;
import tech.ytsaurus.client.rows.WireRowsetDeserializer;
import tech.ytsaurus.core.rows.YTreeRowSerializer;
import tech.ytsaurus.core.rows.YTreeSerializer;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.ysontree.YTreeNode;


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
