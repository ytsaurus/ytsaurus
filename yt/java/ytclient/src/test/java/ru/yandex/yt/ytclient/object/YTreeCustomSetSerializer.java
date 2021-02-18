package ru.yandex.yt.ytclient.object;

import java.util.HashSet;
import java.util.Set;

import ru.yandex.inside.yt.kosher.impl.ytree.object.YTreeSerializer;
import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.AbstractYTreeSerializerForCollections;

public class YTreeCustomSetSerializer<T> extends AbstractYTreeSerializerForCollections<T, Set<T>> {

    public YTreeCustomSetSerializer(YTreeSerializer<T> elemSerializer) {
        super(elemSerializer);
    }

    @Override
    public Set<T> getEmptyImmutableCollection() {
        return Set.of();
    }

    @Override
    public Set<T> getCollection(int initialCapacity) {
        return new HashSet<T>(initialCapacity);
    }

    @Override
    public Set<T> copyCollection(Set<T> values) {
        return new HashSet<>(values);
    }
}
