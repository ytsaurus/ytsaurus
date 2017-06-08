package ru.yandex.yt.rpc.handlers;

import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ReplayingDecoder;
import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.yandex.yt.rpc.protocol.bus.BusPackage;

/**
 * @author valri
 */
public class BusDecoder extends ReplayingDecoder {
    private static final Logger logger = LoggerFactory.getLogger(BusDecoder.class);

    public BusDecoder() {
        setSingleDecode(true);
    }

    @SuppressWarnings("OverlyComplexMethod")
    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf tmp, List<Object> out) throws Exception {
        /*
            DEFINE_ENUM_WITH_UNDERLYING_TYPE(EPacketType, i16,
                ((ExtraMessage)(0))
                ((Ack)    (1))
            );

            DEFINE_BIT_ENUM_WITH_UNDERLYING_TYPE(EPacketFlags, ui16,
                ((None)      (0x0000))
                ((RequestAck)(0x0001))
            );

            struct TPacketHeader
            {
                // Should be equal to PacketSignature.
                ui32 Signature;
                EPacketType Type;
                EPacketFlags Flags;
                TPacketId PacketId;
                ui32 PartCount;
                ui64 Checksum;
            };

            Variable-sized header:
                ui32 PartSizes[PartCount];
                ui64 PartChecksums[PartCount];
                ui64 Checksum;
        */
        ByteBuf in = tmp.order(ByteOrder.LITTLE_ENDIAN);
        if (in.readableBytes() < BusPackage.SMALLEST_BUS_PACKAGE_BYTES_LEN) {
            return;
        }
        in.markReaderIndex();
        final int signature = in.readInt();
        final short type = in.readShort();
        final short flags = in.readShort();
        final long part1 = in.readLong();
        final long part2 = in.readLong();
        final int partCount = in.readInt();
        final long checkSum = in.readLong();
        final List<List<Byte>> res = new ArrayList<>();
        final UUID packId = new UUID(part1, part2);
        if (type == BusPackage.PacketType.MESSAGE.getValue()) {
            if ((partCount < 0) || (partCount > BusPackage.MAX_PART_COUNT)
                    || (in.readableBytes() < (partCount * ((2 * Integer.BYTES) + Long.BYTES))))
            {
                logger.error("Invalid partCount parameter in bus package");
                out.add(new BusPackage(signature, type, flags, packId, checkSum, res));
                return;
            }
            final int[] partSizes = new int[partCount];
            for (int i = 0; i < partCount; ++i) {
                partSizes[i] = in.readInt();
            }
            for (int i = 0; i < partCount; ++i) {
                // handle checksums
                in.readLong();
            }
            in.readLong();

            for (int i = 0; i < partCount; ++i) {
                if (partSizes[i] < 0) continue;
                final byte[] blobPart = new byte[partSizes[i]];
                if (in.readableBytes() >= partSizes[i]) in.readBytes(blobPart);
                res.add(new ArrayList<>(Arrays.asList(ArrayUtils.toObject(blobPart))));
            }
            out.add(new BusPackage(signature, type, flags, packId, checkSum, res));
        } else if (type == BusPackage.PacketType.ACK.getValue()) {
            out.add(new BusPackage(signature, type, flags, packId, checkSum, res));
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        logger.error("Error raised during decoding bus packege", cause);
    }
}

