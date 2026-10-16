package tech.ytsaurus.core.rows;

import java.util.Collection;

/**
 * Exposes collection-specific operations needed by serializers that copy or compose collection values.
 */
public interface YTreeSerializerForCollections<T, C extends Collection<T>> {

    YTreeSerializer<T> getComponent();

    C getEmptyImmutableCollection();

    C getCollection(int initialCapacity);

    C copyCollection(C values);
}
