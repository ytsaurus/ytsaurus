package ru.yandex.yt.rpc.protocol.bus;

import java.util.List;
import java.util.UUID;

/**
 * @author valri
 */
public class BusMessage extends BusPackage {
    public BusMessage(final boolean ackEnabled, final UUID uuid, final boolean enableChecksum,
                      final List<List<Byte>> message)
    {
        super(PacketType.MESSAGE, BusPackage.PacketFlags.fromFlag((short) (ackEnabled ? 1 : 0)),
                uuid, enableChecksum, message);
    }
}
