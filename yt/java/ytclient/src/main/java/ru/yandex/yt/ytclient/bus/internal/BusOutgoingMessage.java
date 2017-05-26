package ru.yandex.yt.ytclient.bus.internal;

import java.util.List;
import java.util.Objects;

import ru.yandex.yt.ytclient.bus.BusDeliveryTracking;
import ru.yandex.yt.ytclient.misc.YtGuid;

/**
 * Временное представление исходящего сообщения
 */
public final class BusOutgoingMessage {
    private final YtGuid packetId;
    private final List<byte[]> message;
    private final BusDeliveryTracking level;

    public BusOutgoingMessage(List<byte[]> message, BusDeliveryTracking level) {
        this.packetId = YtGuid.create();
        this.message = Objects.requireNonNull(message);
        this.level = Objects.requireNonNull(level);
    }

    public YtGuid getPacketId() {
        return packetId;
    }

    public List<byte[]> getMessage() {
        return message;
    }

    public BusDeliveryTracking getLevel() {
        return level;
    }
}
