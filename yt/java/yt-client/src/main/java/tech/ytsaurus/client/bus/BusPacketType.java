package tech.ytsaurus.client.bus;

import java.util.HashMap;
import java.util.Map;

/**
 * Тип пакета в протоколе bus (i16 в C++)
 */
enum BusPacketType {
    MESSAGE(0),
    ACK(1);

    private final short value;

    private static final Map<Short, BusPacketType> INDEX = new HashMap<>();

    BusPacketType(int value) {
        this.value = (short) value;
    }

    public short getValue() {
        return value;
    }

    public static BusPacketType fromValue(short value) {
        BusPacketType busPacketType = INDEX.get(value);
        if (busPacketType == null) {
            throw new IllegalArgumentException("Unsupported bus packet type " + value);
        }
        return busPacketType;
    }

    static {
        for (BusPacketType busPacketType : BusPacketType.values()) {
            INDEX.put(busPacketType.getValue(), busPacketType);
        }
    }
}
