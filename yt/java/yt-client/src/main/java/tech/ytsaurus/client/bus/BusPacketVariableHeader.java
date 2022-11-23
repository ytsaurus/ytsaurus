package tech.ytsaurus.client.bus;

import java.nio.ByteOrder;
import java.util.List;

import io.netty.buffer.ByteBuf;
import tech.ytsaurus.client.misc.YtCrc64;

class BusPacketVariableHeader {
    private final int[] sizes;
    private final long[] checksums;

    BusPacketVariableHeader(int[] sizes, long[] checksums) {
        if (sizes.length != checksums.length) {
            throw new IllegalArgumentException("sizes and checksums must have identical sizes");
        }
        this.sizes = sizes;
        this.checksums = checksums;
    }

    public int getSize(int index) {
        return sizes[index];
    }

    public long getChecksum(int index) {
        return checksums[index];
    }

    public int partCount() {
        return sizes.length;
    }

    /**
     * ui32 PartSizes[PartCount];
     * ui64 PartChecksums[PartCount];
     * ui64 Checksum;
     */
    public static int size(int partCount) {
        return 12 * partCount + 8;
    }

    public static BusPacketVariableHeader readFrom(BusPacketFixedHeader header, ByteBuf in, boolean verifyChecksums) {
        in = in.order(ByteOrder.LITTLE_ENDIAN);
        int start = in.readerIndex();
        int[] sizes = new int[header.getPartCount()];
        for (int i = 0; i < header.getPartCount(); i++) {
            sizes[i] = in.readInt();
        }
        long[] checksums = new long[header.getPartCount()];
        for (int i = 0; i < header.getPartCount(); i++) {
            checksums[i] = in.readLong();
        }
        int end = in.readerIndex();
        long checksum = in.readLong();
        if (verifyChecksums && checksum != BusPacket.NULL_CHECKSUM) {
            long actualChecksum = YtCrc64.fromBytes(in.slice(start, end - start));
            if (actualChecksum != checksum) {
                throw new IllegalStateException(
                        "Packet " + header.getPacketId() + " variable header checksum mismatch: expected=0x" + Long
                                .toUnsignedString(checksum, 16) + ", actual=0x" + Long
                                .toUnsignedString(actualChecksum, 16));
            }
        }
        for (int i = 0; i < header.getPartCount(); i++) {
            if (sizes[i] != BusPacket.NULL_PART_SIZE && (sizes[i] < 0 || sizes[i] > BusPacket.MAX_PART_SIZE)) {
                throw new IllegalStateException(
                        "Packet has part with " + sizes[i] + " bytes, maximum allowed is " + BusPacket.MAX_PART_SIZE);
            }
        }
        return new BusPacketVariableHeader(sizes, checksums);
    }

    public static void writeTo(List<byte[]> message, ByteBuf out, boolean computeChecksums) {
        out = out.order(ByteOrder.LITTLE_ENDIAN);
        int start = out.writerIndex();
        for (byte[] part : message) {
            int size = part != null ? part.length : BusPacket.NULL_PART_SIZE;
            out.writeInt(size);
        }
        for (byte[] part : message) {
            long checksum = computeChecksums && part != null ? YtCrc64.fromBytes(part) : BusPacket.NULL_CHECKSUM;
            out.writeLong(checksum);
        }
        int end = out.writerIndex();
        long checksum = computeChecksums ? YtCrc64.fromBytes(out.slice(start, end - start)) : BusPacket.NULL_CHECKSUM;
        out.writeLong(checksum);
    }
}
