package tech.ytsaurus.client.rows;

import tech.ytsaurus.skiff.WireType;
import tech.ytsaurus.typeinfo.TiType;

class WireTypeUtil {
    private WireTypeUtil() {
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
        } else if (tiType.isUint8()) {
            return WireType.UINT_8;
        } else if (tiType.isUint16()) {
            return WireType.UINT_16;
        } else if (tiType.isUint32()) {
            return WireType.UINT_32;
        } else if (tiType.isUint64()) {
            return WireType.UINT_64;
        } else if (tiType.isDouble()) {
            return WireType.DOUBLE;
        } else if (tiType.isBool()) {
            return WireType.BOOLEAN;
        } else if (tiType.isUtf8()) {
            return WireType.STRING_32;
        } else if (tiType.isString()) {
            return WireType.STRING_32;
        } else if (tiType.isUuid()) {
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
