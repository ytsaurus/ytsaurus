package tech.ytsaurus.core.rows.simple;

import tech.ytsaurus.core.rows.YTreeSerializer;
import tech.ytsaurus.typeinfo.TiType;
import tech.ytsaurus.yson.YsonConsumer;
import tech.ytsaurus.ysontree.YTreeEntityNode;
import tech.ytsaurus.ysontree.YTreeNode;

public class YTreeByteSerializer implements YTreeSerializer<Byte> {

    @Override
    public void serialize(Byte value, YsonConsumer consumer) {
        consumer.onInteger(value);
    }

    @Override
    public Byte deserialize(YTreeNode node) {
        if (node instanceof YTreeEntityNode) {
            return 0;
        }
        int value = node.intValue();
        if (value < Byte.MIN_VALUE || value > Byte.MAX_VALUE) {
            throw new IllegalArgumentException("Value is out of byte range: " + value);
        }
        return (byte) value;
    }

    @Override
    public TiType getColumnValueType() {
        return TiType.int64();
    }
}
