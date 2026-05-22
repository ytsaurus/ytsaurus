package tech.ytsaurus.core.rows.simple;

import tech.ytsaurus.core.rows.YTreeSerializer;
import tech.ytsaurus.typeinfo.TiType;
import tech.ytsaurus.yson.YsonConsumer;
import tech.ytsaurus.ysontree.YTreeEntityNode;
import tech.ytsaurus.ysontree.YTreeNode;

/**
 * Serializer for Java enums represented by their constant names.
 */
public class YTreeEnumSerializer<E extends Enum<E>> implements YTreeSerializer<E> {
    private final Class<E> clazz;

    public YTreeEnumSerializer(Class<E> clazz) {
        this.clazz = clazz;
    }

    public Class<E> getClazz() {
        return clazz;
    }

    @Override
    public void serialize(E obj, YsonConsumer consumer) {
        consumer.onString(obj.name());
    }

    @Override
    public E deserialize(YTreeNode node) {
        if (node instanceof YTreeEntityNode) {
            return null;
        }
        return E.valueOf(clazz, node.stringValue());
    }

    @Override
    public TiType getColumnValueType() {
        return TiType.string();
    }
}
