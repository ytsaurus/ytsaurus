package ru.yandex.yt.ytclient.bus;

import java.net.SocketAddress;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.ServerChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

public class DefaultBusConnector implements BusConnector {
    private final NioEventLoopGroup group;
    private final boolean groupOwner;
    private Duration readTimeout = Duration.ofMinutes(2);
    private Duration writeTimeout = Duration.ofMinutes(2);
    private boolean verifyChecksums = false;
    private boolean calculateChecksums = false;

    public DefaultBusConnector() {
        this(new NioEventLoopGroup(1), true);
    }

    public DefaultBusConnector(NioEventLoopGroup group) {
        this(group, false);
    }

    public DefaultBusConnector(NioEventLoopGroup group, boolean groupOwner) {
        this.group = Objects.requireNonNull(group);
        this.groupOwner = groupOwner;
    }

    public NioEventLoopGroup getGroup() {
        return group;
    }

    public Duration getReadTimeout() {
        return readTimeout;
    }

    public Duration getWriteTimeout() {
        return writeTimeout;
    }

    public boolean isVerifyChecksums() {
        return verifyChecksums;
    }

    public boolean isCalculateChecksums() {
        return calculateChecksums;
    }

    public DefaultBusConnector setReadTimeout(Duration readTimeout) {
        this.readTimeout = Objects.requireNonNull(readTimeout);
        return this;
    }

    public DefaultBusConnector setWriteTimeout(Duration writeTimeout) {
        this.writeTimeout = Objects.requireNonNull(writeTimeout);
        return this;
    }

    public DefaultBusConnector setVerifyChecksums(boolean verifyChecksums) {
        this.verifyChecksums = verifyChecksums;
        return this;
    }

    public DefaultBusConnector setCalculateChecksums(boolean calculateChecksums) {
        this.calculateChecksums = calculateChecksums;
        return this;
    }

    private Bootstrap newBootstrap(BusListener listener) {
        return new Bootstrap()
                .group(group)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .option(ChannelOption.TCP_NODELAY, true)
                .handler(new DefaultBusInitializer(listener)
                        .setReadTimeout(readTimeout)
                        .setWriteTimeout(writeTimeout)
                        .setVerifyChecksums(verifyChecksums)
                        .setCalculateChecksums(calculateChecksums));
    }

    @Override
    public Bus connect(SocketAddress address, BusListener listener) {
        ChannelFuture f = newBootstrap(listener).connect(address);
        try {
            DefaultBusChannel bus = DefaultBusChannel.getOrCreateInstance(f.channel());
            f.addListener((ChannelFuture ready) -> {
                if (ready.isSuccess()) {
                    bus.channelConnected();
                } else {
                    try {
                        bus.channelFailed(ready.cause());
                    } finally {
                        bus.close();
                    }
                }
            });
            return bus;
        } catch (Throwable e) {
            f.cancel(false);
            throw e;
        }
    }

    private ServerBootstrap newServerBootstrap(BusListener listener) {
        return new ServerBootstrap()
                .group(group)
                .channel(NioServerSocketChannel.class)
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childHandler(new DefaultBusInitializer(listener)
                        .setReadTimeout(readTimeout)
                        .setWriteTimeout(writeTimeout));
    }

    @Override
    public BusServer listen(SocketAddress address, BusListener listener) {
        ChannelFuture f = newServerBootstrap(listener).bind(address);
        try {
            DefaultBusServerChannel server = new DefaultBusServerChannel((ServerChannel) f.channel());
            f.addListener(ready -> {
                if (ready.isSuccess()) {
                    server.listenSucceeded();
                } else {
                    try {
                        server.listenFailed(ready.cause());
                    } finally {
                        server.close();
                    }
                }
            });
            return server;
        } catch (Throwable e) {
            // Отменяем bind, если нам не удалось подписаться на результат
            f.cancel(false);
            throw e;
        }
    }

    @Override
    public void close() {
        if (groupOwner) {
            // N.B.: Если не указать quietPeriod=0, то shutdown растягивается на многие секунды
            group.shutdownGracefully(0, 500, TimeUnit.MILLISECONDS).syncUninterruptibly();
        }
    }

    @Override
    public ScheduledExecutorService executorService() {
        return group;
    }
}
