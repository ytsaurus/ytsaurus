package ru.yandex.yt.ytclient.object;

import java.util.Set;

import ru.yandex.bolts.collection.Cf;
import ru.yandex.inside.yt.kosher.impl.ytree.object.YTreeSerializer;
import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.AbstractYTreeCollectionSerializer;

public class YTreeCustomSetSerializer<T> extends AbstractYTreeCollectionSerializer<T, Set<T>> {

    public YTreeCustomSetSerializer(YTreeSerializer<T> elemSerializer) {
        super(elemSerializer);
    }

    @Override
    public Set<T> getEmptyCollection() {
        return Cf.set();
    }

    @Override
    public Set<T> getCollection(int size) {
        return Cf.hashSetWithExpectedSize(size);
    }

    @Override
    public Set<T> copyCollection(Set<T> values) {
        return Cf.toHashSet(values);
    }
}
