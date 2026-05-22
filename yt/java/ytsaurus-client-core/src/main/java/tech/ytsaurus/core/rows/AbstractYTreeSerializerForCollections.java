package tech.ytsaurus.core.rows;

import java.util.Collection;
import java.util.List;

import tech.ytsaurus.typeinfo.TiType;
import tech.ytsaurus.yson.YsonConsumer;
import tech.ytsaurus.ysontree.YTreeEntityNode;
import tech.ytsaurus.ysontree.YTreeMapNode;
import tech.ytsaurus.ysontree.YTreeNode;

/**
 * Base serializer for collection types represented as YSON lists.
 */
public abstract class AbstractYTreeSerializerForCollections<T, C extends Collection<T>> implements
        YTreeSerializer<C>, YTreeSerializerForCollections<T, C> {

    private final YTreeSerializer<T> elemSerializer;

    public AbstractYTreeSerializerForCollections(YTreeSerializer<T> elemSerializer) {
        this.elemSerializer = elemSerializer;
    }

    @Override
    public YTreeSerializer<T> getComponent() {
        return elemSerializer;
    }

    @Override
    public void serialize(C list, YsonConsumer consumer) {
        consumer.onBeginList();
        for (T elem : list) {
            consumer.onListItem();
            elemSerializer.serialize(elem, consumer);
        }
        consumer.onEndList();
    }

    @Override
    public C deserialize(YTreeNode node) {
        if (node instanceof YTreeEntityNode) {
            return getEmptyImmutableCollection();
        }
        final List<YTreeNode> list = node.asList();
        if (list.isEmpty()) {
            return getEmptyImmutableCollection();
        }
        final C result = getCollection(list.size());
        for (YTreeNode value : list) {
            result.add(elemSerializer.deserialize(value));
        }
        return result;
    }

    @Override
    public C deserializeForField(YTreeObjectField field, YTreeMapNode node) {
        if (field.isAttribute) {
            if (node.containsAttribute(field.key)) {
                return deserialize(node.getAttributeOrThrow(field.key));
            } else {
                return getEmptyImmutableCollection();
            }
        } else {
            if (node.containsKey(field.key)) {
                return deserialize(node.getOrThrow(field.key));
            } else {
                return getEmptyImmutableCollection();
            }
        }
    }

    @Override
    public boolean deserializationFieldsAreMandatory() {
        return false;
    }

    @Override
    public TiType getColumnValueType() {
        return TiType.optional(TiType.yson());
    }

}
