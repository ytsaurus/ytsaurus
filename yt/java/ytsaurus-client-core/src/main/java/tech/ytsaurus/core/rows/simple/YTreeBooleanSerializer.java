package tech.ytsaurus.core.rows.simple;

import tech.ytsaurus.core.rows.YTreeSerializer;
import tech.ytsaurus.typeinfo.TiType;
import tech.ytsaurus.yson.YsonConsumer;
import tech.ytsaurus.ysontree.YTreeEntityNode;
import tech.ytsaurus.ysontree.YTreeNode;

/**
 * Serializer for boolean values.
 */
public class YTreeBooleanSerializer implements YTreeSerializer<Boolean> {

    @Override
    public void serialize(Boolean value, YsonConsumer consumer) {
        consumer.onBoolean(value);
    }

    @Override
    public Boolean deserialize(YTreeNode node) {
        if (node instanceof YTreeEntityNode) {
            return false;
        }
        return node.boolValue();
    }

    @Override
    public TiType getColumnValueType() {
        return TiType.bool();
    }

}
