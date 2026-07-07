package tech.ytsaurus.core.rows.simple;

import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.core.rows.YTreeSerializer;
import tech.ytsaurus.typeinfo.TiType;
import tech.ytsaurus.yson.YsonConsumer;
import tech.ytsaurus.ysontree.YTreeNode;
import tech.ytsaurus.ysontree.YTreeNodeUtils;


public class YTreeYPathSerializer implements YTreeSerializer<YPath> {
    @Override
    public void serialize(YPath obj, YsonConsumer consumer) {
        YTreeNodeUtils.walk(obj.toTree(), consumer, true);
    }

    @Override
    public YPath deserialize(YTreeNode node) {
        return YPath.fromTree(node);
    }

    @Override
    public TiType getColumnValueType() {
        return TiType.optional(TiType.yson());
    }
}
