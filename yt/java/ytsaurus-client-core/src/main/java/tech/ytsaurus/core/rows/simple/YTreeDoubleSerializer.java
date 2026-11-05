package tech.ytsaurus.core.rows.simple;

import tech.ytsaurus.core.rows.YTreeSerializer;
import tech.ytsaurus.typeinfo.TiType;
import tech.ytsaurus.yson.YsonConsumer;
import tech.ytsaurus.ysontree.YTreeEntityNode;
import tech.ytsaurus.ysontree.YTreeNode;

/**
 * Serializer for double values.
 */
public class YTreeDoubleSerializer implements YTreeSerializer<Double> {

    @Override
    public void serialize(Double value, YsonConsumer consumer) {
        consumer.onDouble(value);
    }

    @Override
    public Double deserialize(YTreeNode node) {
        if (node instanceof YTreeEntityNode) {
            return 0D;
        }
        if (node.isIntegerNode()) {
            return (double) node.longValue();
        }
        return node.doubleValue();
    }

    @Override
    public TiType getColumnValueType() {
        return TiType.doubleType();
    }

}
