package tech.ytsaurus.client.bus;

import java.util.ArrayList;
import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ytsaurus.client.misc.YtCrc64;

class BusPacketDecoder extends ByteToMessageDecoder {
    private static final Logger logger = LoggerFactory.getLogger(BusPacketDecoder.class);
    private static final byte[] EMPTY_PART = new byte[0];

    private final BusPartDecoder partDecoder = new BusPartDecoder();
    private final boolean verifyChecksums;
    private State state = State.FIXED_HEADER;
    private BusPacketFixedHeader header;
    private BusPacketVariableHeader vheader;
    private List<byte[]> message;

    private enum State {
        FIXED_HEADER,
        VARIABLE_HEADER,
        PARTS,
        ERROR
    }

    BusPacketDecoder() {
        this(true);
    }

    BusPacketDecoder(boolean verifyChecksums) {
        this.verifyChecksums = verifyChecksums;
    }

    /**
     * Подготавливает декодер к чтению следующего пакета
     */
    private void reset() {
        state = State.FIXED_HEADER;
        header = null;
        vheader = null;
        message = null;
    }

    /**
     * Подготавливает декодер к чтению очередной части сообщения
     * <p>
     * Возвращает true, если нужно читать следующую часть используя partDecoder
     * Возвращает false, если больше не нужно ничего читать
     */
    private boolean startNextMessagePart() {
        while (message.size() < header.getPartCount()) {
            int size = vheader.getSize(message.size());
            if (size == BusPacket.NULL_PART_SIZE) {
                message.add(null);
            } else if (size == 0) {
                message.add(EMPTY_PART);
            } else {
                partDecoder.start(size);
                return true;
            }
        }
        return false;
    }

    /**
     * Вычитывает данные для текущей части сообщения
     * <p>
     * Возвращает true, если все данные текущей части успешно прочитаны
     * Возвращает false, если требуется прочитать больше данных
     */
    private boolean readMessagePart(ByteBuf in) {
        byte[] part = partDecoder.read(in);
        if (part != null) {
            if (verifyChecksums) {
                int index = message.size();
                long checksum = vheader.getChecksum(index);
                if (checksum != BusPacket.NULL_CHECKSUM) {
                    long actualChecksum = YtCrc64.fromBytes(part);
                    if (actualChecksum != checksum) {
                        throw new IllegalStateException("Packet " + header.getPacketId() + " part " + index
                                + " checksum mismatch: expected=0x" + Long.toUnsignedString(checksum, 16)
                                + ", actual=0x" + Long.toUnsignedString(actualChecksum, 16));
                    }
                }
            }
            message.add(part);
            return true;
        }
        return false;
    }

    /**
     * Возвращает обработку MESSAGE пакета и подготавливает декодер к чтению следующего пакета
     */
    private BusPacket finishMessage() {
        BusPacket packet = new BusPacket(header.getType(), header.getFlags(), header.getPacketId(), message);
        logger.trace("Received packet: {}", packet);
        reset();
        return packet;
    }

    /**
     * Завершает обработку ACK пакета и подготавливает декодер к чтению следующего пакета
     */
    private BusPacket finishAck() {
        BusPacket packet = new BusPacket(header.getType(), header.getFlags(), header.getPacketId());
        logger.trace("Received packet: {}", packet);
        reset();
        return packet;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        try {
            switch (state) {
                case FIXED_HEADER:
                    if (in.readableBytes() < BusPacketFixedHeader.SIZE) {
                        return;
                    }
                    header = BusPacketFixedHeader.readFrom(in, verifyChecksums);
                    switch (header.getType()) {
                        case MESSAGE:
                            // will read a variable header below
                            state = State.VARIABLE_HEADER;
                            break;
                        case ACK:
                            out.add(finishAck());
                            return;
                        default:
                            throw new IllegalStateException(
                                    "Packet " + header.getPacketId() + " has unexpected type " + header.getType());
                    }
                    // fall through to the next state
                case VARIABLE_HEADER:
                    if (in.readableBytes() < BusPacketVariableHeader.size(header.getPartCount())) {
                        return;
                    }
                    vheader = BusPacketVariableHeader.readFrom(header, in, verifyChecksums);
                    message = new ArrayList<>(header.getPartCount());
                    if (!startNextMessagePart()) {
                        out.add(finishMessage());
                        return;
                    }
                    state = State.PARTS;
                    // fall through to the next state
                case PARTS:
                    while (readMessagePart(in)) {
                        if (!startNextMessagePart()) {
                            out.add(finishMessage());
                            return;
                        }
                    }
                    // Ждём поступления новых данных
                    break;
                case ERROR:
                    // Ошибка протокола, не пытаемся ничего декодировать
                    break;
                default:
                    throw new IllegalStateException("Decoder has unexpected state " + state);
            }
        } catch (Throwable e) {
            state = State.ERROR;
            throw e;
        }
    }
}
