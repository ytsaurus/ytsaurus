package tech.ytsaurus.skiff;

import java.util.Map;

import tech.ytsaurus.ysontree.YTreeNode;

public class WireTypeUtil {
    private static final Map<Class<?>, WireType> SIMPLE_TYPES_MAP = Map.ofEntries(
            Map.entry(byte.class, WireType.INT_8),
            Map.entry(Byte.class, WireType.INT_8),
            Map.entry(short.class, WireType.INT_16),
            Map.entry(Short.class, WireType.INT_16),
            Map.entry(int.class, WireType.INT_32),
            Map.entry(Integer.class, WireType.INT_32),
            Map.entry(long.class, WireType.INT_64),
            Map.entry(Long.class, WireType.INT_64),
            Map.entry(double.class, WireType.DOUBLE),
            Map.entry(Double.class, WireType.DOUBLE),
            Map.entry(boolean.class, WireType.BOOLEAN),
            Map.entry(Boolean.class, WireType.BOOLEAN),
            Map.entry(String.class, WireType.STRING_32)
    );

    private WireTypeUtil() {
    }

    public static WireType getClassWireType(Class<?> clazz) {
        if (YTreeNode.class.isAssignableFrom(clazz)) {
            return WireType.YSON_32;
        }
        return SIMPLE_TYPES_MAP.getOrDefault(clazz, WireType.TUPLE);
    }
}
