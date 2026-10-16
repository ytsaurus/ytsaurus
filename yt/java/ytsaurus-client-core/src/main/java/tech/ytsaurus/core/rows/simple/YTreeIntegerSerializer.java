package tech.ytsaurus.core.rows.simple;

import tech.ytsaurus.core.rows.YTreeSerializer;
import tech.ytsaurus.typeinfo.TiType;
import tech.ytsaurus.yson.YsonConsumer;
import tech.ytsaurus.ysontree.YTreeEntityNode;
import tech.ytsaurus.ysontree.YTreeNode;

/**
 * Serializer for integer values.
 */
public class YTreeIntegerSerializer implements YTreeSerializer<Integer> {

    @Override
    public void serialize(Integer value, YsonConsumer consumer) {
        consumer.onInteger(value);
    }

    @Override
    public Integer deserialize(YTreeNode node) {
        if (node instanceof YTreeEntityNode) {
            return 0;
        }
        return node.intValue();
    }

    @Override
    public TiType getColumnValueType() {
        return TiType.int64();
    }

}
