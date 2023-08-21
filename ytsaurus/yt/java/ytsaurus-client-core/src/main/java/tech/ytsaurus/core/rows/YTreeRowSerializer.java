package tech.ytsaurus.core.rows;

import javax.annotation.Nullable;

import tech.ytsaurus.lang.NonNullApi;
import tech.ytsaurus.lang.NonNullFields;
import tech.ytsaurus.yson.YsonConsumer;


/**
 * Serializer for T that can represent a table row. For example: YTreeMapNode or YTreeObject
 */
@NonNullApi
@NonNullFields
public interface YTreeRowSerializer<T> extends YTreeSerializer<T> {
    void serializeRow(T obj, YsonConsumer consumer, boolean keyFieldsOnly, @Nullable T compareWith);
}
