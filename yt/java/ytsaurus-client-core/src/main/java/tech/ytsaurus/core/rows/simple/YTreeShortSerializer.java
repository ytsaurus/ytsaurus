package tech.ytsaurus.core.rows.simple;

import tech.ytsaurus.core.rows.YTreeSerializer;
import tech.ytsaurus.typeinfo.TiType;
import tech.ytsaurus.yson.YsonConsumer;
import tech.ytsaurus.ysontree.YTreeEntityNode;
import tech.ytsaurus.ysontree.YTreeNode;

public class YTreeShortSerializer implements YTreeSerializer<Short> {

    @Override
    public void serialize(Short value, YsonConsumer consumer) {
        consumer.onInteger(value);
    }

    @Override
    public Short deserialize(YTreeNode node) {
        if (node instanceof YTreeEntityNode) {
            return 0;
        }
        int value = node.intValue();
        if (value < Short.MIN_VALUE || value > Short.MAX_VALUE) {
            throw new IllegalArgumentException("Value is out of short range: " + value);
        }
        return (short) value;
    }

    @Override
    public TiType getColumnValueType() {
        return TiType.int64();
    }
}
