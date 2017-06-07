package ru.yandex.yt.rpc.channel;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.timeout.ReadTimeoutHandler;

import ru.yandex.yt.rpc.handlers.BusDecoder;
import ru.yandex.yt.rpc.handlers.BusEncoder;
import ru.yandex.yt.rpc.handlers.ChannelPoolHandler;

/**
 * @author valri
 */
public class ClientChannelInitializer extends ChannelInitializer<SocketChannel> {
    private int timeoutSeconds;

    public ClientChannelInitializer(int timeout) {
        this.timeoutSeconds = timeout;
    }

    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
        final ChannelPipeline pipeline = ch.pipeline();
        pipeline.addLast(new LoggingHandler(LogLevel.INFO));
        pipeline.addLast(new ReadTimeoutHandler(timeoutSeconds));
        pipeline.addLast(new BusEncoder());
        pipeline.addLast(new BusDecoder());
        pipeline.addLast(new ChannelPoolHandler());
    }
}
