package tech.ytsaurus.client.bus;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

import io.netty.buffer.ByteBuf;
import tech.ytsaurus.core.GUID;

/**
 * Десериализованное представление пакета bus
 */
class BusPacket {
    public static final int PACKET_SIGNATURE = 0x78616d4f;
    public static final int MAX_PART_COUNT = 1 << 28;
    public static final long NULL_CHECKSUM = 0;
    public static final int MAX_PART_SIZE = 256 * 1024 * 1024;
    public static final int NULL_PART_SIZE = -1;

    private final BusPacketType type;
    private final short flags;
    private final GUID packetId;
    private final List<byte[]> message;

    /**
     * Создаёт пакет без приаттаченных данных
     */
    BusPacket(BusPacketType type, short flags, GUID packetId) {
        this(type, flags, packetId, Collections.emptyList());
    }

    /**
     * Создаёт пакет с опционально приаттаченным сообщением
     */
    BusPacket(BusPacketType type, short flags, GUID packetId, List<byte[]> message) {
        this.type = Objects.requireNonNull(type);
        this.flags = flags;
        this.packetId = Objects.requireNonNull(packetId);
        this.message = Objects.requireNonNull(message);
    }

    public BusPacketType getType() {
        return type;
    }

    public short getFlags() {
        return flags;
    }

    public GUID getPacketId() {
        return packetId;
    }

    public List<byte[]> getMessage() {
        return message;
    }

    public boolean hasFlags(short flags) {
        return (this.flags & flags) == flags;
    }

    public int getHeadersSize() {
        int headersSize = BusPacketFixedHeader.SIZE;
        if (type == BusPacketType.MESSAGE) {
            headersSize += BusPacketVariableHeader.size(message.size());
        }
        return headersSize;
    }

    public void writeHeadersTo(ByteBuf out, boolean computeChecksums) {
        new BusPacketFixedHeader(type, flags, packetId, message.size()).writeTo(out, computeChecksums);
        if (type == BusPacketType.MESSAGE) {
            BusPacketVariableHeader.writeTo(message, out, computeChecksums);
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder()
                .append("BusPacket{\n")
                .append("    type=").append(type).append(",\n")
                .append("    flags=").append(flags).append(",\n")
                .append("    packetId=").append(packetId);
        for (int i = 0; i < message.size(); i++) {
            sb.append(",\n    message[").append(i).append("]=");
            byte[] part = message.get(i);
            if (part != null) {
                sb.append('{');
                BusUtil.encodeHex(sb, part);
                sb.append('}');
            } else {
                sb.append("null");
            }
        }
        sb.append('}');
        return sb.toString();
    }
}
