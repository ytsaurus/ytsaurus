package tech.ytsaurus.core.rows;

import java.util.Collections;
import java.util.Map;

import tech.ytsaurus.typeinfo.TiType;
import tech.ytsaurus.yson.YsonConsumer;
import tech.ytsaurus.ysontree.YTreeMapNode;
import tech.ytsaurus.ysontree.YTreeNode;

/**
 * @author sankear
 */
public interface YTreeSerializer<T> {

    void serialize(T obj, YsonConsumer consumer);

    default void serializeForField(YTreeObjectField field, T obj, YsonConsumer consumer) {
        consumer.onKeyedItem(field.key);
        serialize(obj, consumer);
    }

    default Map<String, YTreeObjectField<?>> getFieldMap() {
        return Collections.emptyMap();
    }

    default Class<T> getClazz() {
        throw new IllegalStateException();
    }

    T deserialize(YTreeNode node);

    default T deserializeForField(YTreeObjectField field, YTreeMapNode node) {
        if (field.isAttribute) {
            return deserialize(node.getAttributeOrThrow(field.key));
        } else {
            return deserialize(node.getOrThrow(field.key));
        }
    }

    default boolean deserializationFieldsAreMandatory() {
        return true;
    }

    default <U> YTreeSerializer<U> uncheckedCast() {
        //noinspection unchecked
        return (YTreeSerializer<U>) this;
    }

    TiType getColumnValueType();

}
