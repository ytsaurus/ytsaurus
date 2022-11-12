package ru.yandex.inside.yt.kosher.impl.ytree.object.serializers;

import tech.ytsaurus.type_info.TiType;
import tech.ytsaurus.yson.YsonConsumer;
import tech.ytsaurus.ysontree.YTreeMapNode;
import tech.ytsaurus.ysontree.YTreeNode;
import tech.ytsaurus.ysontree.YTreeNodeUtils;

import ru.yandex.inside.yt.kosher.impl.ytree.object.YTreeRowSerializer;

public class YTreeMapNodeSerializer implements YTreeRowSerializer<YTreeMapNode> {
    private Class<YTreeMapNode> clazz;

    public YTreeMapNodeSerializer(Class<YTreeMapNode> clazz) {
        this.clazz = clazz;
    }

    @Override
    public void serialize(YTreeMapNode obj, YsonConsumer consumer) {
        YTreeNodeUtils.walk(obj, consumer, true);
    }

    @Override
    public void serializeRow(YTreeMapNode obj, YsonConsumer consumer, boolean keyFieldsOnly, YTreeMapNode compareWith) {
        serialize(obj, consumer);
    }

    @Override
    public YTreeMapNode deserialize(YTreeNode node) {
        return node.mapNode();
    }

    @Override
    public TiType getColumnValueType() {
        return TiType.optional(TiType.yson());
    }

    @Override
    public Class<YTreeMapNode> getClazz() {
        return clazz;
    }
}
