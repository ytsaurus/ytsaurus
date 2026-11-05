package tech.ytsaurus.core.rows.simple;

import tech.ytsaurus.core.StringValueEnum;
import tech.ytsaurus.core.StringValueEnumResolver;
import tech.ytsaurus.core.rows.YTreeSerializer;
import tech.ytsaurus.typeinfo.TiType;
import tech.ytsaurus.yson.YsonConsumer;
import tech.ytsaurus.ysontree.YTreeNode;


public class YTreeStringValueEnumSerializer<E extends Enum<E> & StringValueEnum> implements YTreeSerializer<E> {
    private final StringValueEnumResolver<E> resolver;

    public YTreeStringValueEnumSerializer(StringValueEnumResolver<E> resolver) {
        this.resolver = resolver;
    }

    @Override
    public void serialize(E obj, YsonConsumer consumer) {
        consumer.onString(obj.value());
    }

    @Override
    public E deserialize(YTreeNode node) {
        return resolver.fromName(node.stringValue());
    }

    @Override
    public TiType getColumnValueType() {
        return TiType.string();
    }
}
