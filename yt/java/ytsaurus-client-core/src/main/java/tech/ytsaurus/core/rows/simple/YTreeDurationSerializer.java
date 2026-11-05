package tech.ytsaurus.core.rows.simple;

import java.time.Duration;

import tech.ytsaurus.core.rows.YTreeSerializer;
import tech.ytsaurus.typeinfo.TiType;
import tech.ytsaurus.yson.YsonConsumer;
import tech.ytsaurus.ysontree.YTreeEntityNode;
import tech.ytsaurus.ysontree.YTreeNode;


public class YTreeDurationSerializer implements YTreeSerializer<Duration> {
    @Override
    public void serialize(Duration value, YsonConsumer consumer) {
        consumer.onInteger(value.toMillis());
    }

    @Override
    public Duration deserialize(YTreeNode node) {
        return node instanceof YTreeEntityNode ? Duration.ZERO : Duration.ofMillis(node.longValue());
    }

    @Override
    public TiType getColumnValueType() {
        return TiType.int64();
    }
}
