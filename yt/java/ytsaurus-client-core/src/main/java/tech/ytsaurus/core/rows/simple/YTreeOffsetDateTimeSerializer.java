package tech.ytsaurus.core.rows.simple;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;

import tech.ytsaurus.core.rows.YTreeSerializer;
import tech.ytsaurus.typeinfo.TiType;
import tech.ytsaurus.yson.YsonConsumer;
import tech.ytsaurus.ysontree.YTreeEntityNode;
import tech.ytsaurus.ysontree.YTreeNode;


public class YTreeOffsetDateTimeSerializer implements YTreeSerializer<OffsetDateTime> {

    @Override
    public void serialize(OffsetDateTime value, YsonConsumer consumer) {
        consumer.onInteger(value.toInstant().toEpochMilli());
    }

    @Override
    public OffsetDateTime deserialize(YTreeNode node) {
        return OffsetDateTime.ofInstant(Instant.ofEpochMilli(node instanceof YTreeEntityNode ? 0L : node.longValue()),
                ZoneId.systemDefault());
    }

    @Override
    public TiType getColumnValueType() {
        return TiType.int64();
    }
}
