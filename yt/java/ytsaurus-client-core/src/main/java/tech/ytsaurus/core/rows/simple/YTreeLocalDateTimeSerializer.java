package tech.ytsaurus.core.rows.simple;

import java.time.LocalDateTime;

import tech.ytsaurus.core.rows.YTreeSerializer;
import tech.ytsaurus.typeinfo.TiType;
import tech.ytsaurus.yson.YsonConsumer;
import tech.ytsaurus.ysontree.YTreeEntityNode;
import tech.ytsaurus.ysontree.YTreeNode;

/**
 * Serializer for {@link LocalDateTime} values represented as ISO-8601 strings.
 */
public class YTreeLocalDateTimeSerializer implements YTreeSerializer<LocalDateTime> {

    @Override
    public void serialize(LocalDateTime obj, YsonConsumer consumer) {
        consumer.onString(obj.toString());
    }

    @Override
    public LocalDateTime deserialize(YTreeNode node) {
        return node instanceof YTreeEntityNode ?
            null :
            LocalDateTime.parse(node.stringValue());
    }

    @Override
    public TiType getColumnValueType() {
        return TiType.string();
    }
}
