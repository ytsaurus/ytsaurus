package ru.yandex.inside.yt.kosher.impl.ytree.object;

import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;
import ru.yandex.yson.YsonConsumer;

/**
 * Serializer for T that can represent a table row. For example: YTreeMapNode or YTreeObject
 */
@NonNullApi
@NonNullFields
public interface YTreeRowSerializer<T> extends YTreeSerializer<T> {
    void serializeRow(T obj, YsonConsumer consumer, boolean keyFieldsOnly, T compareWith);
}
