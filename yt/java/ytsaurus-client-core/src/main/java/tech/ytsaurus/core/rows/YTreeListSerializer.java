package tech.ytsaurus.core.rows;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Serializer for {@link List} values represented as YSON lists.
 */
public class YTreeListSerializer<T> extends AbstractYTreeSerializerForCollections<T, List<T>> {

    private final Type elementType;

    public YTreeListSerializer(Type elementType, YTreeSerializer<T> elemSerializer) {
        super(elemSerializer);
        this.elementType = elementType;
    }

    public Type getElementType() {
        return elementType;
    }

    @Override
    public List<T> getEmptyImmutableCollection() {
        return Collections.emptyList();
    }

    @Override
    public List<T> getCollection(int initialCapacity) {
        return new ArrayList<>(initialCapacity);
    }

    @Override
    public List<T> copyCollection(List<T> values) {
        return new ArrayList<>(values);
    }
}
