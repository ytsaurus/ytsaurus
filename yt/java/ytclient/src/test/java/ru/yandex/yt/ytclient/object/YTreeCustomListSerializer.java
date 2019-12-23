package ru.yandex.yt.ytclient.object;

import java.util.List;

import ru.yandex.bolts.collection.Cf;
import ru.yandex.inside.yt.kosher.impl.ytree.object.YTreeSerializer;
import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.AbstractYTreeCollectionSerializer;

public class YTreeCustomListSerializer<T> extends AbstractYTreeCollectionSerializer<T, List<T>> {

    public YTreeCustomListSerializer(YTreeSerializer<T> elemSerializer) {
        super(elemSerializer);
    }

    @Override
    public List<T> getEmptyCollection() {
        return Cf.list();
    }

    @Override
    public List<T> getCollection(int size) {
        return Cf.arrayListWithCapacity(size);
    }

    @Override
    public List<T> copyCollection(List<T> values) {
        return Cf.toArrayList(values);
    }
}
