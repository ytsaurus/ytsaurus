package ru.yandex.yt.ytclient.bus.internal;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.DecoderException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import ru.yandex.inside.yt.kosher.common.GUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

@RunWith(Parameterized.class)
public class BusPacketDecoderTest {
    @Parameterized.Parameter
    public boolean paranoid;

    @Parameterized.Parameters(name = "paranoid={0}")
    public static Object[][] params() {
        return new Object[][]{
                new Object[]{false},
                new Object[]{true}};
    }

    private static final GUID SAMPLE_PACKET_ID = new GUID(0x0001020304050607L, 0x08090a0b0c0d0e0fL);

    private static final String SAMPLE_PACKET_DATA
            = "4F6D6178" // signature
            + "0000" // packet type
            + "0100" // packet flags
            + "07060504030201000F0E0D0C0B0A0908" // packet id
            + "01000000" // part count
            + "A6EF6D674C741AD3" // header checksum
            + "04000000" // part size
            + "6645C7675923A45C" // part checksum
            + "BD8828B4785610C3" // variable header checksum
            + "2A2B2C2D";

    private static final String SAMPLE_PACKET_NO_CHECKSUMS_DATA
            = "4F6D6178" // signature
            + "0000" // packet type
            + "0100" // packet flags
            + "07060504030201000F0E0D0C0B0A0908" // packet id
            + "01000000" // part count
            + "0000000000000000" // header checksum
            + "04000000" // part size
            + "0000000000000000" // part checksum
            + "0000000000000000" // variable header checksum
            + "2A2B2C2D";

    private static final String SAMPLE_PACKET_NULL_PART_DATA
            = "4F6D6178" // signature
            + "0000" // packet type
            + "0100" // packet flags
            + "07060504030201000F0E0D0C0B0A0908" // packet id
            + "02000000" // part count
            + "0000000000000000" // header checksum
            + "FFFFFFFF" // part size
            + "04000000" // part size
            + "0000000000000000" // part checksum
            + "0000000000000000" // part checksum
            + "0000000000000000" // variable header checksum
            + "2A2B2C2D";

    private static final String SAMPLE_PACKET_CORRUPTED_CHECKSUM_DATA
            = "4F6D6178" // signature
            + "0000" // packet type
            + "0100" // packet flags
            + "07060504030201000F0E0D0C0B0A0908" // packet id
            + "01000000" // part count
            + "FEFEFEFEFEFEFEFE" // header checksum
            + "04000000" // part size
            + "FEFEFEFEFEFEFEFE" // part checksum
            + "FEFEFEFEFEFEFEFE" // variable header checksum
            + "2A2B2C2D";

    private void verifyDecoder(EmbeddedChannel channel, String hexBytes, BusPacket expectedPacket) {
        byte[] bytes = unhexString(hexBytes);
        List<ByteBuf> buffers = new ArrayList<>();
        if (paranoid) {
            // Бьём все данные по одному байту
            for (byte value : bytes) {
                buffers.add(Unpooled.wrappedBuffer(new byte[]{value}));
            }
        } else {
            buffers.add(Unpooled.wrappedBuffer(bytes));
        }
        for (ByteBuf buffer : buffers) {
            assertThat("all buffers have refcnt=1", buffer.refCnt(), is(1));
            channel.writeInbound(buffer);
        }
        for (ByteBuf buffer : buffers) {
            assertThat("all buffers are released", buffer.refCnt(), is(0));
        }
        BusPacket packet = (BusPacket) channel.readInbound();
        assertThat("Packet types match", packet.getType(), is(expectedPacket.getType()));
        assertThat("Packet flags match", packet.getFlags(), is(expectedPacket.getFlags()));
        assertThat("Packet ids match", packet.getPacketId(), is(expectedPacket.getPacketId()));
        assertThat("Message sizes match", packet.getMessage().size(), is(expectedPacket.getMessage().size()));
        for (int i = 0; i < packet.getMessage().size(); i++) {
            byte[] part = packet.getMessage().get(i);
            byte[] expectedPart = expectedPacket.getMessage().get(i);
            assertThat("Part " + i + " data match", part, is(expectedPart));
        }
        assertThat("a single packet is decoded", channel.readInbound(), is(nullValue()));
    }

    private BusPacket makeSamplePacket(byte[]... parts) {
        return new BusPacket(BusPacketType.MESSAGE, BusPacketFlags.REQUEST_ACK, SAMPLE_PACKET_ID, Arrays.asList(parts));
    }

    @Test
    public void decoderTest() throws InterruptedException {
        EmbeddedChannel channel = new EmbeddedChannel(new BusPacketDecoder());
        try {
            verifyDecoder(channel, SAMPLE_PACKET_DATA, makeSamplePacket(new byte[]{42, 43, 44, 45}));
        } finally {
            channel.close().sync();
        }
    }

    @Test
    public void decoderTestWithoutChecksums() throws InterruptedException {
        EmbeddedChannel channel = new EmbeddedChannel(new BusPacketDecoder());
        try {
            verifyDecoder(channel, SAMPLE_PACKET_NO_CHECKSUMS_DATA, makeSamplePacket(new byte[]{42, 43, 44, 45}));
        } finally {
            channel.close().sync();
        }
    }

    @Test
    public void decoderTestNullPart() throws InterruptedException {
        EmbeddedChannel channel = new EmbeddedChannel(new BusPacketDecoder());
        try {
            verifyDecoder(channel, SAMPLE_PACKET_NULL_PART_DATA, makeSamplePacket(null, new byte[]{42, 43, 44, 45}));
        } finally {
            channel.close().sync();
        }
    }

    @Test(expected = DecoderException.class)
    public void decoderTestCorruptedChecksum() throws InterruptedException {
        EmbeddedChannel channel = new EmbeddedChannel(new BusPacketDecoder());
        try {
            verifyDecoder(channel, SAMPLE_PACKET_CORRUPTED_CHECKSUM_DATA, makeSamplePacket(new byte[]{42, 43, 44, 45}));
        } finally {
            channel.close().sync();
        }
    }

    @Test
    public void decoderTestIgnoreCorruptedChecksum() throws InterruptedException {
        EmbeddedChannel channel = new EmbeddedChannel(new BusPacketDecoder(false));
        try {
            verifyDecoder(channel, SAMPLE_PACKET_CORRUPTED_CHECKSUM_DATA, makeSamplePacket(new byte[]{42, 43, 44, 45}));
        } finally {
            channel.close().sync();
        }
    }

    private static final String DIGITS = "0123456789ABCDEF";

    private static byte[] unhexString(String s) {
        if (s.length() % 2 != 0) {
            throw new IllegalArgumentException("Not a hex string: " + s);
        }
        int size = s.length() / 2;
        byte[] data = new byte[size];
        for (int i = 0; i < size; ++i) {
            char c = s.charAt(i * 2);
            int a = DIGITS.indexOf(c);
            if (a == -1) {
                throw new IllegalArgumentException("Not a hex string: " + s);
            }
            c = s.charAt(i * 2 + 1);
            int b = DIGITS.indexOf(c);
            if (b == -1) {
                throw new IllegalArgumentException("Not a hex string: " + s);
            }
            data[i] = (byte) ((a << 4) + b);
        }
        return data;
    }

    private static byte[] readAll(ByteBuf buf) {
        if (buf != null) {
            byte[] data = new byte[buf.readableBytes()];
            buf.getBytes(buf.readerIndex(), data);
            return data;
        } else {
            return null;
        }
    }
}
