package ru.yandex.yt.rpc.handlers;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.yandex.yt.rpc.protocol.bus.BusPackage;

/**
 * @author valri
 */
public class BusEncoder extends MessageToByteEncoder<BusPackage> {
    private static final Logger logger = LoggerFactory.getLogger(BusEncoder.class);

    @Override
    protected void encode(ChannelHandlerContext ctx, BusPackage msg, ByteBuf out) throws Exception {
        out.writeBytes(msg.getBytes());
        logger.debug("Bus package ({}) encoded successfully", msg.toString());
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        logger.error("Error raised during encoding bus package", cause);
    }
}
