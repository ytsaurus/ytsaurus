package ru.yandex.yt.rpc.handlers;

import java.util.concurrent.CompletableFuture;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.yandex.yt.rpc.protocol.bus.BusPackage;

/**
 * @author valri
 */
public class ChannelPoolHandler extends SimpleChannelInboundHandler<BusPackage> {
    private static final Logger logger = LoggerFactory.getLogger(ChannelPoolHandler.class.getName());

    CompletableFuture<BusPackage> future;

    public void setCompletableFuture(CompletableFuture<BusPackage> cf) {
        this.future = cf;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, BusPackage msg) throws Exception {
        if (msg.getType() == BusPackage.PacketType.ACK) {
            logger.debug("ACK received for request ({})", msg.toString());
        } else {
            if (msg.getFlags() == BusPackage.PacketFlags.REQUEST_ACK) {
                logger.debug("RPC server requested ACK for response ({})", msg.toString());
                ctx.writeAndFlush(new BusPackage(msg).makeACK()).addListener(f -> {
                    if (f.isSuccess()) {
                        logger.debug("Successfully send ACK for response ({})", msg.toString());
                    } else {
                        logger.warn("Failed to send ACK for response ({})", msg.toString());
                    }
                    future.complete(msg);
                });
            } else {
                future.complete(msg);
            }
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        logger.error("Error handling RPC server request", cause);
        if (!future.isDone()) {
            future.completeExceptionally(cause);
        }
    }
}
