package tech.ytsaurus.core.rows.simple;

import tech.ytsaurus.core.rows.YTreeSerializer;
import tech.ytsaurus.typeinfo.TiType;
import tech.ytsaurus.yson.YsonConsumer;
import tech.ytsaurus.ysontree.YTreeEntityNode;
import tech.ytsaurus.ysontree.YTreeNode;

/**
 * Serializer for string values.
 */
public class YTreeStringSerializer implements YTreeSerializer<String> {

    @Override
    public void serialize(String value, YsonConsumer consumer) {
        consumer.onString(value);
    }

    @Override
    public String deserialize(YTreeNode node) {
        if (node instanceof YTreeEntityNode) {
            return "";
        }
        return node.stringValue();
    }

    @Override
    public TiType getColumnValueType() {
        return TiType.string();
    }

}
