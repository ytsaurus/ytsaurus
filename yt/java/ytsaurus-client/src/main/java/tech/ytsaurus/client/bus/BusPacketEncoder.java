package tech.ytsaurus.client.bus;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class BusPacketEncoder extends ChannelOutboundHandlerAdapter {
    private static final Logger logger = LoggerFactory.getLogger(BusPacketEncoder.class);

    private final boolean computeChecksums;

    BusPacketEncoder() {
        this(true);
    }

    BusPacketEncoder(boolean computeChecksums) {
        this.computeChecksums = computeChecksums;
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (msg instanceof BusPacket) {
            BusPacket packet = (BusPacket) msg;
            logger.trace("Sending packet: {}", packet);

            // Пишем заголовки отдельным прямым io буфером
            ByteBuf buf = ctx.alloc().ioBuffer(packet.getHeadersSize());
            try {
                packet.writeHeadersTo(buf, computeChecksums);
            } catch (Throwable e) {
                buf.release();
                throw e;
            }
            ctx.write(buf);

            if (packet.getType() == BusPacketType.MESSAGE) {
                // Для сообщений пишем каждую часть по отдельности
                for (byte[] part : packet.getMessage()) {
                    if (part != null && part.length > 0) {
                        ctx.write(Unpooled.wrappedBuffer(part));
                    }
                }
            }

            // В конце добавляем пустой буфер для правильного завершения promise
            ctx.write(Unpooled.EMPTY_BUFFER, promise);
        } else {
            super.write(ctx, msg, promise);
        }
    }
}
