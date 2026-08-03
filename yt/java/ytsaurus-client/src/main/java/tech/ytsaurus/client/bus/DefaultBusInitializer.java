package tech.ytsaurus.client.bus;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.proxy.HttpProxyHandler;
import io.netty.handler.timeout.ReadTimeoutHandler;
import io.netty.handler.timeout.WriteTimeoutHandler;

/**
 * Класс для инициализации каналов для работы с bus протоколом
 */
class DefaultBusInitializer extends ChannelInitializer<Channel> {
    private final BusListener listener;
    private Duration readTimeout = Duration.ZERO;
    private Duration writeTimeout = Duration.ZERO;
    private boolean verifyChecksums = false;
    private boolean calculateChecksums = false;
    @Nullable
    private InetSocketAddress httpConnectProxy;

    private final DefaultBusChannelMetricsHolder metricsHolder;

    DefaultBusInitializer(BusListener listener) {
        this(listener, DefaultBusChannelMetricsHolderImpl.INSTANCE);
    }

    DefaultBusInitializer(BusListener listener, DefaultBusChannelMetricsHolder metricsHolder) {
        this.listener = listener;
        this.metricsHolder = metricsHolder;
    }

    public DefaultBusInitializer setReadTimeout(Duration readTimeout) {
        this.readTimeout = Objects.requireNonNull(readTimeout);
        return this;
    }

    public DefaultBusInitializer setWriteTimeout(Duration writeTimeout) {
        this.writeTimeout = Objects.requireNonNull(writeTimeout);
        return this;
    }

    public DefaultBusInitializer setVerifyChecksums(boolean verifyChecksums) {
        this.verifyChecksums = verifyChecksums;
        return this;
    }

    public DefaultBusInitializer setCalculateChecksums(boolean calculateChecksums) {
        this.calculateChecksums = calculateChecksums;
        return this;
    }

    public DefaultBusInitializer setHttpConnectProxy(@Nullable InetSocketAddress httpConnectProxy) {
        this.httpConnectProxy = httpConnectProxy;
        return this;
    }

    @Override
    protected void initChannel(Channel channel) throws Exception {
        DefaultBusChannel bus = DefaultBusChannel.getOrCreateInstance(channel, metricsHolder);
        ChannelPipeline pipeline = channel.pipeline();
        if (httpConnectProxy != null) {
            // Rebuild the address so that the proxy hostname is resolved anew for every connection.
            pipeline.addLast("proxy", new HttpProxyHandler(
                    new InetSocketAddress(httpConnectProxy.getHostString(), httpConnectProxy.getPort())));
        }
        long readTimeoutNanos = readTimeout.toNanos();
        if (readTimeoutNanos > 0) {
            pipeline.addLast("read-timeout", new ReadTimeoutHandler(readTimeoutNanos, TimeUnit.NANOSECONDS));
        }
        long writeTimeoutNanos = writeTimeout.toNanos();
        if (writeTimeoutNanos > 0) {
            pipeline.addLast("write-timeout", new WriteTimeoutHandler(writeTimeoutNanos, TimeUnit.NANOSECONDS));
        }
        pipeline
                .addLast("decoder", new BusPacketDecoder(verifyChecksums))
                .addLast("encoder", new BusPacketEncoder(calculateChecksums))
                .addLast("handler", new BusProtocolHandler(bus, listener));
    }
}
