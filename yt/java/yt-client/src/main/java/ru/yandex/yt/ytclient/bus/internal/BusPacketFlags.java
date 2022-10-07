package ru.yandex.yt.ytclient.bus.internal;

/**
 * Флаги пакета в протоколе bus (ui16 в C++)
 */
public class BusPacketFlags {
    public static final short NONE = 0;
    public static final short REQUEST_ACK = 1;

    private BusPacketFlags() {
    }
}
