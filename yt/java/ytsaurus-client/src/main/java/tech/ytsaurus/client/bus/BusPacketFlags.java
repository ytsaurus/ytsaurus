package tech.ytsaurus.client.bus;

/**
 * Флаги пакета в протоколе bus (ui16 в C++)
 */
class BusPacketFlags {
    public static final short NONE = 0;
    public static final short REQUEST_ACK = 1;

    private BusPacketFlags() {
    }
}
