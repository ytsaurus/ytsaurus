package ru.yandex.yt.rpc.handlers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import com.google.common.primitives.Bytes;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.embedded.EmbeddedChannel;
import org.apache.commons.lang3.ArrayUtils;
import org.junit.Test;

import ru.yandex.yt.rpc.protocol.bus.BusPackage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static ru.yandex.yt.rpc.utils.Utility.byteArrayFromList;

/**
 * @author valri
 */
public class CodecTest {
    @Test
    public void decoderTestNoAttach() throws Exception {
        final EmbeddedChannel ch = new EmbeddedChannel(new BusEncoder(), new BusDecoder());
        final ByteBuf out = ByteBufUtil.threadLocalDirectBuffer();
        final UUID uuid = UUID.randomUUID();
        final int numOfParts = 0;

        out.writeInt(ByteBufUtil.swapInt(BusPackage.DEFAULT_SIGNATURE));
        out.writeShort(ByteBufUtil.swapShort(BusPackage.PacketType.ACK.getValue()));
        out.writeShort(ByteBufUtil.swapShort(BusPackage.PacketFlags.NONE.getValue()));
        out.writeLong(ByteBufUtil.swapLong(uuid.getMostSignificantBits()));
        out.writeLong(ByteBufUtil.swapLong((uuid.getLeastSignificantBits())));
        out.writeInt(ByteBufUtil.swapInt(numOfParts));
        out.writeLong(BusPackage.NULL_CHECKSUM);
        ch.writeInbound(out);
        ch.writeInbound(ch.releaseOutbound());

        final BusPackage resultBusPackage = ch.readInbound();
        assertEquals(BusPackage.DEFAULT_SIGNATURE, resultBusPackage.getSignature());
        assertEquals(BusPackage.PacketType.ACK, resultBusPackage.getType());
        assertEquals(BusPackage.PacketFlags.NONE, resultBusPackage.getFlags());
        assertEquals(uuid, resultBusPackage.getUuid());
        assertEquals(numOfParts, resultBusPackage.getNumberOfParts());
        assertEquals(BusPackage.NULL_CHECKSUM, resultBusPackage.getCheckSum());
        assertFalse(resultBusPackage.hasBlobPart());
    }

    @Test
    public void decoderTestWithAttach() throws Exception {
        final EmbeddedChannel ch = new EmbeddedChannel(new BusEncoder(), new BusDecoder());
        final ByteBuf out = ByteBufUtil.threadLocalDirectBuffer();
        final UUID uuid = UUID.randomUUID();
        final byte[] attachBytes = "iam a cute attachment".getBytes();
        final int numOfParts = 1;
        final List<Byte> headerPart = Bytes.asList(attachBytes);

        out.writeInt(ByteBufUtil.swapInt(BusPackage.DEFAULT_SIGNATURE));
        out.writeShort(ByteBufUtil.swapShort(BusPackage.PacketType.MESSAGE.getValue()));
        out.writeShort(ByteBufUtil.swapShort(BusPackage.PacketFlags.REQUEST_ACK.getValue()));
        out.writeLong(ByteBufUtil.swapLong(uuid.getMostSignificantBits()));
        out.writeLong(ByteBufUtil.swapLong((uuid.getLeastSignificantBits())));
        out.writeInt(ByteBufUtil.swapInt(numOfParts));
        out.writeLong(BusPackage.NULL_CHECKSUM);
        out.writeInt(ByteBufUtil.swapInt(attachBytes.length));
        out.writeLong(ByteBufUtil.swapLong(BusPackage.NULL_CHECKSUM));
        out.writeLong(ByteBufUtil.swapLong(BusPackage.NULL_CHECKSUM));
        out.writeBytes(byteArrayFromList(headerPart));

        ch.writeInbound(out);
        ch.writeInbound(ch.releaseOutbound());

        final BusPackage resultBusPackage = ch.readInbound();
        assertEquals(BusPackage.DEFAULT_SIGNATURE, resultBusPackage.getSignature());
        assertEquals(BusPackage.PacketType.MESSAGE, resultBusPackage.getType());
        assertEquals(BusPackage.PacketFlags.REQUEST_ACK, resultBusPackage.getFlags());
        assertEquals(uuid, resultBusPackage.getUuid());
        assertEquals(BusPackage.NULL_CHECKSUM, resultBusPackage.getCheckSum());
        assertEquals(numOfParts, resultBusPackage.getNumberOfParts());
        assertEquals(resultBusPackage.getHeaderMessage(), headerPart);
    }

    @Test
    public void encoderTestWithAttach() throws Exception {
        final EmbeddedChannel ch = new EmbeddedChannel(new BusEncoder(), new BusDecoder());
        final UUID uuid = UUID.randomUUID();
        final byte[] attachBytes = "iam a cute attachment".getBytes();
        final List<Byte> headerPart = Bytes.asList(attachBytes);
        final List<List<Byte>> pack = new ArrayList<>();
        pack.add(headerPart);
        BusPackage request = new BusPackage(BusPackage.PacketType.MESSAGE, BusPackage.PacketFlags.NONE,
                uuid, false, pack);

        ch.writeOutbound(request);
        ch.writeOutbound(ch.releaseInbound());

        final ByteBuf incomingBuffer = ch.readOutbound();
        incomingBuffer.markReaderIndex();
        assertEquals(BusPackage.DEFAULT_SIGNATURE, ByteBufUtil.swapInt(incomingBuffer.readInt()));
        assertEquals(BusPackage.PacketType.MESSAGE.getValue(),  ByteBufUtil.swapShort(incomingBuffer.readShort()));
        assertEquals(BusPackage.PacketFlags.NONE.getValue(),  ByteBufUtil.swapShort(incomingBuffer.readShort()));
        final long part1 = ByteBufUtil.swapLong(incomingBuffer.readLong());
        final long part2 = ByteBufUtil.swapLong(incomingBuffer.readLong());
        assertEquals(uuid, new UUID(part1, part2));
        assertEquals(pack.size(), ByteBufUtil.swapInt(incomingBuffer.readInt()));
        assertEquals(BusPackage.NULL_CHECKSUM, ByteBufUtil.swapLong(incomingBuffer.readLong()));
        int headerSize = ByteBufUtil.swapInt(incomingBuffer.readInt());
        assertEquals(headerPart.size(), headerSize);
        assertEquals(BusPackage.NULL_CHECKSUM, ByteBufUtil.swapLong(incomingBuffer.readLong()));
        assertEquals(BusPackage.NULL_CHECKSUM, ByteBufUtil.swapLong(incomingBuffer.readLong()));
        final byte[] blobPart = new byte[headerSize];
        incomingBuffer.readBytes(blobPart);
        assertEquals(headerPart, Arrays.asList(ArrayUtils.toObject(blobPart)));
        assertEquals(0, incomingBuffer.readableBytes());
    }

    @Test
    public void encoderTestNoAttach() throws Exception {
        final EmbeddedChannel ch = new EmbeddedChannel(new BusEncoder(), new BusDecoder());
        final UUID uuid = UUID.randomUUID();
        BusPackage request = new BusPackage(BusPackage.PacketType.MESSAGE,
                BusPackage.PacketFlags.NONE, uuid, false, new ArrayList<>());

        ch.writeOutbound(request);
        ch.writeOutbound(ch.releaseInbound());

        ByteBuf in = ch.readOutbound();
        in.markReaderIndex();
        assertEquals(BusPackage.DEFAULT_SIGNATURE, ByteBufUtil.swapInt(in.readInt()));
        assertEquals(BusPackage.PacketType.MESSAGE.getValue(),  ByteBufUtil.swapShort(in.readShort()));
        assertEquals(BusPackage.PacketFlags.NONE.getValue(),  ByteBufUtil.swapShort(in.readShort()));
        final long part1 = ByteBufUtil.swapLong(in.readLong());
        final long part2 = ByteBufUtil.swapLong(in.readLong());
        assertEquals(uuid, new UUID(part1, part2));
        assertEquals(0, ByteBufUtil.swapInt(in.readInt()));
        assertEquals(BusPackage.NULL_CHECKSUM, ByteBufUtil.swapLong(in.readLong()));
        assertEquals(0 , in.readableBytes());
    }
}
