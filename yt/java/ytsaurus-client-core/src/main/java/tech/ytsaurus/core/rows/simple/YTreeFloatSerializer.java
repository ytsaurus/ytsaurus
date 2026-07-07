package tech.ytsaurus.core.rows.simple;

import tech.ytsaurus.core.rows.YTreeSerializer;
import tech.ytsaurus.typeinfo.TiType;
import tech.ytsaurus.yson.YsonConsumer;
import tech.ytsaurus.ysontree.YTreeEntityNode;
import tech.ytsaurus.ysontree.YTreeNode;

/**
 * Serializer for float values.
 */
public class YTreeFloatSerializer implements YTreeSerializer<Float> {

    @Override
    public void serialize(Float value, YsonConsumer consumer) {
        consumer.onDouble(value);
    }

    @Override
    public Float deserialize(YTreeNode node) {
        if (node instanceof YTreeEntityNode) {
            return 0F;
        }
        return node.floatValue();
    }

    @Override
    public TiType getColumnValueType() {
        return TiType.doubleType();
    }

}
