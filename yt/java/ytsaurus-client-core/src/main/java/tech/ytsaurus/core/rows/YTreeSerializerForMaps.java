package tech.ytsaurus.core.rows;

import java.util.Map;

/**
 * Exposes map-specific operations needed by serializers that copy or compose map values.
 */
public interface YTreeSerializerForMaps<T> {

    YTreeSerializer<T> getComponent();

    Map<String, T> getEmptyImmutableMap();

    Map<String, T> getMap(int initialCapacity);

    Map<String, T> copyMap(Map<String, T> values);
}
