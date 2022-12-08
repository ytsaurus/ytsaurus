package tech.ytsaurus.client.bus;

import java.util.List;
import java.util.Objects;

import tech.ytsaurus.core.GUID;

/**
 * Временное представление исходящего сообщения
 */
final class BusOutgoingMessage {
    private final GUID packetId;
    private final List<byte[]> message;
    private final BusDeliveryTracking level;

    BusOutgoingMessage(List<byte[]> message, BusDeliveryTracking level) {
        this.packetId = GUID.create();
        this.message = Objects.requireNonNull(message);
        this.level = Objects.requireNonNull(level);
    }

    public GUID getPacketId() {
        return packetId;
    }

    public List<byte[]> getMessage() {
        return message;
    }

    public BusDeliveryTracking getLevel() {
        return level;
    }
}
