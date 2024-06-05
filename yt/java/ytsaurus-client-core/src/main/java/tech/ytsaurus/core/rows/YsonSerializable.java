package tech.ytsaurus.core.rows;

import tech.ytsaurus.yson.YsonConsumer;
import tech.ytsaurus.ysontree.YTreeNode;

public interface YsonSerializable {
    void serialize(YsonConsumer consumer);

    void deserialize(YTreeNode node);
}
