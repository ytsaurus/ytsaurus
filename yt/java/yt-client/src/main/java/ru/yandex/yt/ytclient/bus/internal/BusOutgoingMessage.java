package ru.yandex.yt.ytclient.bus.internal;

import java.util.List;
import java.util.Objects;

import ru.yandex.inside.yt.kosher.common.GUID;
import ru.yandex.yt.ytclient.bus.BusDeliveryTracking;

/**
 * Временное представление исходящего сообщения
 */
public final class BusOutgoingMessage {
    private final GUID packetId;
    private final List<byte[]> message;
    private final BusDeliveryTracking level;

    public BusOutgoingMessage(List<byte[]> message, BusDeliveryTracking level) {
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
