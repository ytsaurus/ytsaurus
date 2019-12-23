package ru.yandex.yt.ytclient.object;

import java.util.Map;

import ru.yandex.bolts.collection.Cf;
import ru.yandex.inside.yt.kosher.impl.ytree.object.YTreeSerializer;
import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.AbstractYTreeMapSerializer;

public class YTreeCustomMapSerializer<T> extends AbstractYTreeMapSerializer<T> {

    public YTreeCustomMapSerializer(YTreeSerializer<T> valueSerializer) {
        super(valueSerializer);
    }

    @Override
    public Map<String, T> getEmptyMap() {
        return Cf.map();
    }

    @Override
    public Map<String, T> getMap(int size) {
        return Cf.hashMapWithExpectedSize(size);
    }

    @Override
    public Map<String, T> copyMap(Map<String, T> values) {
        return Cf.toHashMap(values);
    }
}
