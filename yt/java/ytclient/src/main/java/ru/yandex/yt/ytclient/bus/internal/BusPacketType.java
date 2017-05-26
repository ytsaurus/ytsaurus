package ru.yandex.yt.ytclient.bus.internal;

import java.util.HashMap;
import java.util.Map;

/**
 * Тип пакета в протоколе bus (i16 в C++)
 */
public enum BusPacketType {
    MESSAGE(0),
    ACK(1);

    private final short value;

    BusPacketType(int value) {
        this.value = (short) value;
    }

    public short getValue() {
        return value;
    }

    public static BusPacketType fromValue(short value) {
        BusPacketType busPacketType = index.get(value);
        if (busPacketType == null) {
            throw new IllegalArgumentException("Unsupported bus packet type " + value);
        }
        return busPacketType;
    }

    private static final Map<Short, BusPacketType> index = new HashMap<>();

    static {
        for (BusPacketType busPacketType : BusPacketType.values()) {
            index.put(busPacketType.getValue(), busPacketType);
        }
    }
}
