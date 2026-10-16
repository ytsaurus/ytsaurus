package tech.ytsaurus.core.rows;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Serializer for {@link Set} values represented as YSON lists.
 */
public class YTreeSetSerializer<T> extends AbstractYTreeSerializerForCollections<T, Set<T>> {

    public YTreeSetSerializer(YTreeSerializer<T> elemSerializer) {
        super(elemSerializer);
    }

    @Override
    public Set<T> getEmptyImmutableCollection() {
        return Collections.emptySet();
    }

    @Override
    public Set<T> getCollection(int initialCapacity) {
        return new HashSet<>(initialCapacity);
    }

    @Override
    public Set<T> copyCollection(Set<T> values) {
        return new HashSet<>(values);
    }
}
