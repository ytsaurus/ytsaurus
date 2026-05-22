package tech.ytsaurus.core.rows.simple;

import tech.ytsaurus.core.rows.YTreeSerializer;
import tech.ytsaurus.typeinfo.TiType;
import tech.ytsaurus.yson.YsonConsumer;
import tech.ytsaurus.ysontree.YTreeEntityNode;
import tech.ytsaurus.ysontree.YTreeNode;

/**
 * Serializer for long values.
 */
public class YTreeLongSerializer implements YTreeSerializer<Long> {

    @Override
    public void serialize(Long value, YsonConsumer consumer) {
        consumer.onInteger(value);
    }

    @Override
    public Long deserialize(YTreeNode node) {
        if (node instanceof YTreeEntityNode) {
            return 0L;
        }
        return node.longValue();
    }

    @Override
    public TiType getColumnValueType() {
        return TiType.int64();
    }

}
