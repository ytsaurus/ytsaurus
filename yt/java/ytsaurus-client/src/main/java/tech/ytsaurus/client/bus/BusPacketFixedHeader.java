package tech.ytsaurus.client.bus;

import java.nio.ByteOrder;
import java.util.Objects;

import io.netty.buffer.ByteBuf;
import tech.ytsaurus.client.misc.YtCrc64;
import tech.ytsaurus.core.GUID;


class BusPacketFixedHeader {
    /**
     * ui32 Signature;
     * EPacketType Type;
     * EPacketFlags Flags;
     * TPacketId PacketId;
     * ui32 PartCount;
     * ui64 Checksum;
     */
    public static final int SIZE = 4 + 2 + 2 + 16 + 4 + 8;

    private final BusPacketType type;
    private final short flags;
    private final GUID packetId;
    private final int partCount;

    BusPacketFixedHeader(BusPacketType type, short flags, GUID packetId, int partCount) {
        this.type = Objects.requireNonNull(type);
        this.flags = flags;
        this.packetId = Objects.requireNonNull(packetId);
        this.partCount = partCount;
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

    public int getPartCount() {
        return partCount;
    }

    public static BusPacketFixedHeader readFrom(ByteBuf in, boolean verifyChecksums) {
        in = in.order(ByteOrder.LITTLE_ENDIAN);
        int start = in.readerIndex();
        int signature = in.readInt();
        if (signature != BusPacket.PACKET_SIGNATURE) {
            throw new IllegalStateException("Packet signature mismatch: " + Integer.toUnsignedString(signature, 16));
        }
        BusPacketType type = BusPacketType.fromValue(in.readShort());
        short flags = in.readShort();
        GUID packetId = BusUtil.readGuidFrom(in);
        int partCount = in.readInt();
        int end = in.readerIndex();
        long checksum = in.readLong();
        if (verifyChecksums && checksum != BusPacket.NULL_CHECKSUM) {
            long actualChecksum = YtCrc64.fromBytes(in.slice(start, end - start));
            if (actualChecksum != checksum) {
                throw new IllegalStateException("Packet " + packetId + " header checksum mismatch: expected=0x" + Long
                        .toUnsignedString(checksum, 16) + ", actual=0x" + Long.toUnsignedString(actualChecksum, 16));
            }
        }
        if (partCount < 0 || partCount > BusPacket.MAX_PART_COUNT) {
            throw new IllegalStateException(
                    "Packet has " + partCount + " part, maximum allowed is " + BusPacket.MAX_PART_COUNT);
        }
        return new BusPacketFixedHeader(type, flags, packetId, partCount);
    }

    public void writeTo(ByteBuf out, boolean computeChecksums) {
        out = out.order(ByteOrder.LITTLE_ENDIAN);
        int start = out.writerIndex();
        out.writeInt(BusPacket.PACKET_SIGNATURE);
        out.writeShort(type.getValue());
        out.writeShort(flags);
        BusUtil.writeTo(out, packetId);
        out.writeInt(partCount);
        int end = out.writerIndex();
        long checksum = BusPacket.NULL_CHECKSUM;
        if (computeChecksums) {
            checksum = YtCrc64.fromBytes(out.slice(start, end - start));
        }
        out.writeLong(checksum);
    }
}
