package tech.ytsaurus.core.rows.simple;

import java.time.Instant;

import tech.ytsaurus.core.rows.YTreeSerializer;
import tech.ytsaurus.typeinfo.TiType;
import tech.ytsaurus.yson.YsonConsumer;
import tech.ytsaurus.ysontree.YTreeEntityNode;
import tech.ytsaurus.ysontree.YTreeNode;


public class YTreeInstantSerializer implements YTreeSerializer<Instant> {
    @Override
    public void serialize(Instant value, YsonConsumer consumer) {
        consumer.onInteger(value.toEpochMilli());
    }

    @Override
    public Instant deserialize(YTreeNode node) {
        return Instant.ofEpochMilli(node instanceof YTreeEntityNode ? 0L : node.longValue());
    }

    @Override
    public TiType getColumnValueType() {
        return TiType.int64();
    }
}

