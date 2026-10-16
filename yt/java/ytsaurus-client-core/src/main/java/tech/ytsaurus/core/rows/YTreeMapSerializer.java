package tech.ytsaurus.core.rows;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Serializer for {@code Map<String, T>} values represented as YSON maps.
 */
public class YTreeMapSerializer<T> extends AbstractYTreeSerializerForMaps<T> {

    public YTreeMapSerializer(YTreeSerializer<T> valueSerializer) {
        super(valueSerializer);
    }

    @Override
    public Map<String, T> getEmptyImmutableMap() {
        return Collections.emptyMap();
    }

    @Override
    public Map<String, T> getMap(int initialCapacity) {
        return new HashMap<>(initialCapacity);
    }

    @Override
    public Map<String, T> copyMap(Map<String, T> values) {
        return new HashMap<>(values);
    }
}
