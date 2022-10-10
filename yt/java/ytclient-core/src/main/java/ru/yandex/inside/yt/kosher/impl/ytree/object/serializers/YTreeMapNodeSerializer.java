package ru.yandex.inside.yt.kosher.impl.ytree.object.serializers;

import ru.yandex.inside.yt.kosher.impl.ytree.YTreeNodeUtils;
import ru.yandex.inside.yt.kosher.impl.ytree.object.YTreeRowSerializer;
import ru.yandex.inside.yt.kosher.ytree.YTreeMapNode;
import ru.yandex.inside.yt.kosher.ytree.YTreeNode;
import ru.yandex.type_info.TiType;
import ru.yandex.yson.YsonConsumer;

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
