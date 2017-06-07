package ru.yandex.yt.rpc.protocol.rpc;

import java.util.Arrays;
import java.util.Map;

import com.google.common.collect.Maps;

/**
 * @author valri
 */
public enum RpcMessageType {
    UNKNOWN(0), REQUEST(0x69637072), REQUEST_CANCELLATION(0x63637072), RESPONSE(0x6f637072);
    private final int value;

    private static final Map<Integer, RpcMessageType> LOOKUP = Maps.uniqueIndex(
            Arrays.asList(RpcMessageType.values()),
            RpcMessageType::getValue
    );

    RpcMessageType(int value) {
        this.value = value;
    }

    public int getValue() {
        return this.value;
    }

    public static RpcMessageType fromType(int type) {
        return LOOKUP.get(type);
    }
};
