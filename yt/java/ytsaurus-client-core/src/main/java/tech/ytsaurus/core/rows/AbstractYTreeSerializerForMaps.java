package tech.ytsaurus.core.rows;

import java.util.Map;

import tech.ytsaurus.typeinfo.TiType;
import tech.ytsaurus.yson.YsonConsumer;
import tech.ytsaurus.ysontree.YTreeEntityNode;
import tech.ytsaurus.ysontree.YTreeMapNode;
import tech.ytsaurus.ysontree.YTreeNode;

/**
 * Base serializer for maps with string keys represented as YSON maps.
 */
public abstract class AbstractYTreeSerializerForMaps<T> implements YTreeSerializer<Map<String, T>>,
        YTreeSerializerForMaps<T> {

    private final YTreeSerializer<T> valueSerializer;

    public AbstractYTreeSerializerForMaps(YTreeSerializer<T> valueSerializer) {
        this.valueSerializer = valueSerializer;
    }

    @Override
    public YTreeSerializer<T> getComponent() {
        return valueSerializer;
    }

    @Override
    public void serialize(Map<String, T> map, YsonConsumer consumer) {
        consumer.onBeginMap();
        for (Map.Entry<String, T> entry : map.entrySet()) {
            consumer.onKeyedItem(entry.getKey());
            valueSerializer.serialize(entry.getValue(), consumer);
        }
        consumer.onEndMap();
    }

    @Override
    public Map<String, T> deserialize(YTreeNode node) {
        if (node instanceof YTreeEntityNode) {
            return getEmptyImmutableMap();
        }
        final Map<String, YTreeNode> map = node.asMap();
        if (map.isEmpty()) {
            return getEmptyImmutableMap();
        }
        final Map<String, T> result = getMap(map.size());
        for (Map.Entry<String, YTreeNode> e : map.entrySet()) {
            result.put(e.getKey(), valueSerializer.deserialize(e.getValue()));
        }
        return result;
    }

    @Override
    public Map<String, T> deserializeForField(YTreeObjectField field, YTreeMapNode node) {
        if (field.isAttribute) {
            if (node.containsAttribute(field.key)) {
                return deserialize(node.getAttributeOrThrow(field.key));
            } else {
                return getEmptyImmutableMap();
            }
        } else {
            if (node.containsKey(field.key)) {
                return deserialize(node.getOrThrow(field.key));
            } else {
                return getEmptyImmutableMap();
            }
        }
    }

    @Override
    public boolean deserializationFieldsAreMandatory() {
        return false;
    }

    @Override
    public TiType getColumnValueType() {
        return TiType.optional(TiType.yson());
    }

}
