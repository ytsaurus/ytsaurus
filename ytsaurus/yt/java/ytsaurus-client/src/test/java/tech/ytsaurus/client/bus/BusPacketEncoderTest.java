package tech.ytsaurus.client.bus;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Test;
import tech.ytsaurus.core.GUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BusPacketEncoderTest {
    private static final char[] DIGITS = "0123456789ABCDEF".toCharArray();

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

    private static final String SAMPLE_PACKET_EMPTY_PART_DATA
            = "4F6D6178" // signature
            + "0000" // packet type
            + "0100" // packet flags
            + "07060504030201000F0E0D0C0B0A0908" // packet id
            + "02000000" // part count
            + "0000000000000000" // header checksum
            + "00000000" // part size
            + "04000000" // part size
            + "0000000000000000" // part checksum
            + "0000000000000000" // part checksum
            + "0000000000000000" // variable header checksum
            + "2A2B2C2D";

    private static final String SAMPLE_PACKET_MULTI_PART_DATA
            = "4F6D6178" // signature
            + "0000" // packet type
            + "0100" // packet flags
            + "07060504030201000F0E0D0C0B0A0908" // packet id
            + "02000000" // part count
            + "0000000000000000" // header checksum
            + "04000000" // part size
            + "05000000" // part size
            + "0000000000000000" // part checksum
            + "0000000000000000" // part checksum
            + "0000000000000000" // variable header checksum
            + "2A2B2C2D"
            + "2E2F303132";

    private void verifyEncoder(EmbeddedChannel channel, BusPacket packet, String expectedBytes) {
        ChannelFuture future = channel.write(packet);
        channel.flush();
        assertTrue("channel future should be successful", future.isSuccess());
        byte[] wireBytes = readAll(channel);
        assertEquals(hexString(wireBytes), expectedBytes);
    }

    private BusPacket makeSamplePacket(byte[]... parts) {
        return new BusPacket(BusPacketType.MESSAGE, BusPacketFlags.REQUEST_ACK, SAMPLE_PACKET_ID, Arrays.asList(parts));
    }

    @Test
    public void encoderTest() throws InterruptedException {
        EmbeddedChannel channel = new EmbeddedChannel(new BusPacketEncoder());
        try {
            verifyEncoder(channel, makeSamplePacket(new byte[]{42, 43, 44, 45}), SAMPLE_PACKET_DATA);
        } finally {
            channel.close().sync();
        }
    }

    @Test
    public void encoderTestWithoutChecksums() throws InterruptedException {
        EmbeddedChannel channel = new EmbeddedChannel(new BusPacketEncoder(false));
        try {
            verifyEncoder(channel, makeSamplePacket(new byte[]{42, 43, 44, 45}), SAMPLE_PACKET_NO_CHECKSUMS_DATA);
        } finally {
            channel.close().sync();
        }
    }

    @Test
    public void encoderTestNullPart() throws InterruptedException {
        EmbeddedChannel channel = new EmbeddedChannel(new BusPacketEncoder(false));
        try {
            verifyEncoder(channel, makeSamplePacket(null, new byte[]{42, 43, 44, 45}), SAMPLE_PACKET_NULL_PART_DATA);
        } finally {
            channel.close().sync();
        }
    }

    @Test
    public void encoderTestEmptyPart() throws InterruptedException {
        EmbeddedChannel channel = new EmbeddedChannel(new BusPacketEncoder(false));
        try {
            verifyEncoder(channel, makeSamplePacket(new byte[]{}, new byte[]{42, 43, 44, 45}),
                    SAMPLE_PACKET_EMPTY_PART_DATA);
        } finally {
            channel.close().sync();
        }
    }

    @Test
    public void encoderTestMultiPart() throws InterruptedException {
        EmbeddedChannel channel = new EmbeddedChannel(new BusPacketEncoder(false));
        try {
            verifyEncoder(channel, makeSamplePacket(new byte[]{42, 43, 44, 45}, new byte[]{46, 47, 48, 49, 50}),
                    SAMPLE_PACKET_MULTI_PART_DATA);
        } finally {
            channel.close().sync();
        }
    }

    private static String hexString(byte[] data) {
        StringBuilder sb = new StringBuilder(data.length * 2);
        for (int i = 0; i < data.length; i++) {
            int b = data[i] & 0xff;
            sb.append(DIGITS[b >> 4]);
            sb.append(DIGITS[b & 15]);
        }
        return sb.toString();
    }

    private static byte[] readAll(EmbeddedChannel channel) {
        int totalSize = 0;
        List<ByteBuf> chunks = new ArrayList<>();
        try {
            while (true) {
                ByteBuf chunk = (ByteBuf) channel.readOutbound();
                if (chunk == null) {
                    break;
                }
                totalSize += chunk.readableBytes();
                chunks.add(chunk);
            }
            int offset = 0;
            byte[] wireBytes = new byte[totalSize];
            for (ByteBuf wireBuf : chunks) {
                int size = wireBuf.readableBytes();
                if (size > 0) {
                    wireBuf.readBytes(wireBytes, offset, size);
                    offset += size;
                }
            }
            return wireBytes;
        } finally {
            for (ByteBuf chunk : chunks) {
                chunk.release();
            }
        }
    }
}
