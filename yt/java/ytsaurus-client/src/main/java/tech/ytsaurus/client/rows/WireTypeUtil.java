package tech.ytsaurus.client.rows;

import java.util.Map;

import tech.ytsaurus.skiff.WireType;
import tech.ytsaurus.typeinfo.TiType;
import tech.ytsaurus.ysontree.YTreeNode;

class WireTypeUtil {
    private static final Map<Class<?>, WireType> CLASS_WIRE_TYPE_MAP = Map.ofEntries(
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

    static WireType getWireTypeOf(Class<?> clazz) {
        if (YTreeNode.class.isAssignableFrom(clazz)) {
            return WireType.YSON_32;
        }
        return CLASS_WIRE_TYPE_MAP.getOrDefault(clazz, WireType.TUPLE);
    }

    static WireType getWireTypeOf(TiType tiType) {
        if (tiType.isInt8()) {
            return WireType.INT_8;
        } else if (tiType.isInt16()) {
            return WireType.INT_16;
        } else if (tiType.isInt32()) {
            return WireType.INT_32;
        } else if (tiType.isInt64()) {
            return WireType.INT_64;
        } else if (tiType.isDouble()) {
            return WireType.DOUBLE;
        } else if (tiType.isBool()) {
            return WireType.BOOLEAN;
        } else if (tiType.isString()) {
            return WireType.STRING_32;
        } else if (tiType.isYson()) {
            return WireType.YSON_32;
        } else if (tiType.isDecimal()) {
            int precision = tiType.asDecimal().getPrecision();
            if (precision <= 9) {
                return WireType.INT_32;
            }
            if (precision <= 18) {
                return WireType.INT_64;
            }
            return WireType.INT_128;
        }
        return WireType.TUPLE;
    }
}
