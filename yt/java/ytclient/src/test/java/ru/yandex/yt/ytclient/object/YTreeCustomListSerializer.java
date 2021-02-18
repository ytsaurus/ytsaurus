package ru.yandex.yt.ytclient.object;

import java.util.ArrayList;
import java.util.List;

import ru.yandex.inside.yt.kosher.impl.ytree.object.YTreeSerializer;
import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.AbstractYTreeSerializerForCollections;

public class YTreeCustomListSerializer<T> extends AbstractYTreeSerializerForCollections<T, List<T>> {

    public YTreeCustomListSerializer(YTreeSerializer<T> elemSerializer) {
        super(elemSerializer);
    }

    @Override
    public List<T> getEmptyImmutableCollection() {
        return List.of();
    }

    @Override
    public List<T> getCollection(int initialCapacity) {
        return new ArrayList<T>(initialCapacity);
    }

    @Override
    public List<T> copyCollection(List<T> values) {
        return new ArrayList<>(values);
    }
}
