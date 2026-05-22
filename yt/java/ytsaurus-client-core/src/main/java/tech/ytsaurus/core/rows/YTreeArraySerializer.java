package tech.ytsaurus.core.rows;

import java.lang.reflect.Array;
import java.lang.reflect.Type;
import java.util.List;

import tech.ytsaurus.core.utils.ClassUtils;
import tech.ytsaurus.typeinfo.TiType;
import tech.ytsaurus.yson.YsonConsumer;
import tech.ytsaurus.ysontree.YTreeEntityNode;
import tech.ytsaurus.ysontree.YTreeNode;

/**
 * Serializer for Java arrays represented as YSON lists.
 */
public class YTreeArraySerializer<A, E> implements YTreeSerializer<A> {
    private final Class<?> elementType;
    private final YTreeSerializer<E> elemSerializer;

    public YTreeArraySerializer(Type componentType, YTreeSerializer<E> elemSerializer) {
        this.elementType = ClassUtils.erasure(componentType);
        this.elemSerializer = elemSerializer;
    }

    /**
     * Returns the array element class, replacing primitive element classes with their wrapper classes.
     */
    public Class<E> getElementWrapperClass() {
        return (Class<E>) YTreeSerializerFactory.wrapPrimitive(elementType);
    }

    public YTreeSerializer<E> getComponent() {
        return elemSerializer;
    }

    @Override
    public void serialize(A array, YsonConsumer consumer) {
        consumer.onBeginList();
        for (int i = 0; i < Array.getLength(array); ++i) {
            consumer.onListItem();
            elemSerializer.serialize((E) Array.get(array, i), consumer);
        }
        consumer.onEndList();
    }

    @Override
    public A deserialize(YTreeNode node) {
        if (node instanceof YTreeEntityNode) {
            return newArray(0);
        }
        List<YTreeNode> list = node.asList();
        A array = newArray(list.size());

        for (int i = 0; i < list.size(); ++i) {
            Array.set(array, i, elemSerializer.deserialize(list.get(i)));
        }
        return array;
    }

    @Override
    public TiType getColumnValueType() {
        return TiType.optional(TiType.yson());
    }

    private A newArray(int length) {
        return (A) Array.newInstance(this.elementType, length);
    }
}
