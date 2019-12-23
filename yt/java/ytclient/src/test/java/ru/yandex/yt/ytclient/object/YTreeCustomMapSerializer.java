package ru.yandex.yt.ytclient.object;

import java.util.Map;

import ru.yandex.bolts.collection.Cf;
import ru.yandex.inside.yt.kosher.impl.ytree.object.YTreeSerializer;
import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.AbstractYTreeSerializerForMaps;

public class YTreeCustomMapSerializer<T> extends AbstractYTreeSerializerForMaps<T> {

    public YTreeCustomMapSerializer(YTreeSerializer<T> valueSerializer) {
        super(valueSerializer);
    }

    @Override
    public Map<String, T> getEmptyImmutableMap() {
        return Cf.map();
    }

    @Override
    public Map<String, T> getMap(int initialCapacity) {
        return Cf.hashMapWithExpectedSize(initialCapacity);
    }

    @Override
    public Map<String, T> copyMap(Map<String, T> values) {
        return Cf.toHashMap(values);
    }
}
