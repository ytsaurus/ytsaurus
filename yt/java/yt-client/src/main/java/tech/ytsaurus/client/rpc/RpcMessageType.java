package tech.ytsaurus.client.rpc;

import java.util.HashMap;
import java.util.Map;

public enum RpcMessageType {
    REQUEST(0x69637072),
    CANCEL(0x63637072),
    RESPONSE(0x6f637072),

    STREAMING_PAYLOAD(0x70637072),
    STREAMING_FEEDBACK(0x66637072),

    HANDSHAKE(0x68737562);

    private static final Map<Integer, RpcMessageType> TYPES = new HashMap<>();

    private final int value;

    RpcMessageType(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    public static RpcMessageType fromValue(int value) {
        RpcMessageType type = TYPES.get(value);
        if (type == null) {
            throw new IllegalArgumentException(
                    "Unsupported rpc message type: 0x" + Integer.toUnsignedString(value, 16));
        }
        return type;
    }

    static {
        for (RpcMessageType type : values()) {
            TYPES.put(type.getValue(), type);
        }
    }
}
