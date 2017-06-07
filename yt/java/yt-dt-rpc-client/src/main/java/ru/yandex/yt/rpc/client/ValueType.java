package ru.yandex.yt.rpc.client;

import java.util.Arrays;
import java.util.Map;

import com.google.common.collect.Maps;

/**
 * C++ equivalent enum EValueType
 * @author valri
 */
public enum ValueType {
    MIN((short) 0x00),
    THE_BOTTOM((short) 0x01),
    NULL((short) 0x02),
    INT_64((short) 0x03),
    UINT_64((short) 0x04),
    DOUBLE((short) 0x05),
    BOOLEAN((short) 0x06),
    STRING((short) 0x10),
    ANY((short) 0x11),
    MAX((short) 0xef);

    private final short value;

    private static final Map<Short, ValueType> LOOKUP = Maps.uniqueIndex(
            Arrays.asList(ValueType.values()),
            ValueType::getValue
    );

    ValueType(short value) {
        this.value = value;
    }

    public Short getValue() {
        return this.value;
    }

    public static ValueType fromType(Short type) {
        return LOOKUP.get(type);
    }

    public static int size() {
        return Integer.BYTES;
    }
};
