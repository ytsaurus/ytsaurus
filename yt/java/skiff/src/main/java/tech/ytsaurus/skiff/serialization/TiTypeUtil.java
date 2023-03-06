package tech.ytsaurus.skiff.serialization;

import java.util.Map;
import java.util.Optional;

import tech.ytsaurus.typeinfo.TiType;
import tech.ytsaurus.ysontree.YTreeNode;

public class TiTypeUtil {
    private static final Map<Class<?>, TiType> SIMPLE_TYPES_MAP = Map.ofEntries(
            Map.entry(byte.class, TiType.int8()),
            Map.entry(Byte.class, TiType.int8()),
            Map.entry(short.class, TiType.int16()),
            Map.entry(Short.class, TiType.int16()),
            Map.entry(int.class, TiType.int32()),
            Map.entry(Integer.class, TiType.int32()),
            Map.entry(long.class, TiType.int64()),
            Map.entry(Long.class, TiType.int64()),
            Map.entry(double.class, TiType.doubleType()),
            Map.entry(Double.class, TiType.doubleType()),
            Map.entry(boolean.class, TiType.bool()),
            Map.entry(Boolean.class, TiType.bool()),
            Map.entry(String.class, TiType.string())
    );

    private TiTypeUtil() {
    }

    public static Optional<TiType> getTiTypeIfSimple(Class<?> clazz) {
        if (YTreeNode.class.isAssignableFrom(clazz)) {
            return Optional.of(TiType.yson());
        }
        return Optional.ofNullable(SIMPLE_TYPES_MAP.get(clazz));
    }
}
