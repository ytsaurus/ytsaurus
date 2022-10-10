package ru.yandex.yt.ytclient;

import ru.yandex.inside.yt.kosher.impl.ytree.object.YTreeRowSerializer;
import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.YTreeMapNodeSerializer;
import ru.yandex.inside.yt.kosher.ytree.YTreeMapNode;
import ru.yandex.yt.ytclient.tables.TableSchema;

public class SerializationResolverImpl implements SerializationResolver {
    public <T> YTreeRowSerializer<T> forClass(Class<T> clazz, TableSchema schema) {
        if (clazz.equals(YTreeMapNode.class)) {
            return (YTreeRowSerializer<T>) new YTreeMapNodeSerializer((Class<YTreeMapNode>) clazz);
        } else {
            throw new IllegalArgumentException("Unsupported class: " + clazz);
        }
    }
}
