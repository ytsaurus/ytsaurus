package ru.yandex.yt.ytclient;

import ru.yandex.inside.yt.kosher.impl.ytree.object.YTreeRowSerializer;
import ru.yandex.yt.ytclient.tables.TableSchema;

public interface SerializationResolver {
    <T> YTreeRowSerializer<T> forClass(Class<T> clazz, TableSchema schema);
}
