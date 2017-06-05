package ru.yandex.yt.ytclient.bus;

import java.net.SocketAddress;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoop;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.yandex.yt.ytclient.bus.internal.BusOutgoingMessage;
import ru.yandex.yt.ytclient.misc.YtGuid;

/**
 * Канал, работающий по протоколу bus
 */
public class DefaultBusChannel implements Bus, BusLifecycle {
    private static final Logger logger = LoggerFactory.getLogger(DefaultBusChannel.class);
    private static final MetricRegistry metrics = SharedMetricRegistries.getOrCreate("ytclient");
    private static final Histogram packetsHistogram = metrics.histogram(MetricRegistry.name(DefaultBusChannel.class, "packets", "histogram"));

    private static final AttributeKey<DefaultBusChannel> CHANNEL_KEY =
            AttributeKey.valueOf(DefaultBusChannel.class.getName());

    private final Channel channel;
    private final ChannelPromise connected;
    private final ChannelPromise disconnected;

    public DefaultBusChannel(Channel channel) {
        this.channel = Objects.requireNonNull(channel);
        this.connected = channel.newPromise();
        this.connected.setUncancellable();
        this.disconnected = channel.newPromise();
        this.disconnected.setUncancellable();
    }

    @Override
    public Channel channel() {
        return channel;
    }

    @Override
    public EventLoop eventLoop() {
        return channel.eventLoop();
    }

    @Override
    public ChannelFuture connected() {
        return connected;
    }

    @Override
    public ChannelFuture disconnected() {
        return disconnected;
    }

    @Override
    public ChannelFuture closed() {
        return channel.closeFuture();
    }

    @Override
    public SocketAddress localAddress() {
        return channel.localAddress();
    }

    @Override
    public SocketAddress remoteAddress() {
        return channel.remoteAddress();
    }

    /**
     * Начинает процедуру закрытия соединения
     */
    @Override
    public CompletableFuture<Void> close() {
        return BusUtil.makeCompletableFuture(channel.close());
    }

    /**
     * Отправляет сообщение с указанным уровнем подтверждения о доставке
     */
    @Override
    public CompletableFuture<Void> send(List<byte[]> message, BusDeliveryTracking level) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        BusOutgoingMessage outgoingMessage = new BusOutgoingMessage(message, level);
        if (connected.isDone()) {
            sendNow(outgoingMessage, result);
        } else {
            connected.addListener(ignored -> {
                if (!result.isDone()) {
                    sendNow(outgoingMessage, result);
                }
            });
        }
        return result;
    }

    private void logWriteResult(YtGuid packetId, Instant started) {
        long elapsed = Duration.between(started, Instant.now()).toMillis();
        logger.debug("(DefaultBusChannel({}@{})) message `{}` sent in {} ms",
            channel.remoteAddress(), hashCode(), packetId, elapsed);
        packetsHistogram.update(elapsed);
    }

    /**
     * Немедленно отправляет сообщение и выставляет результат по завершении
     */
    private void sendNow(BusOutgoingMessage outgoingMessage, CompletableFuture<Void> result) {
        if (connected.cause() != null) {
            logger.debug("(DefaultBusChannel({}@{})) cannot send message `{}`: `{}`",
                channel.remoteAddress(), hashCode(), outgoingMessage.getPacketId(), connected.cause());
            result.completeExceptionally(connected.cause());
        } else {
            Instant started = Instant.now();
            YtGuid packetId = outgoingMessage.getPacketId();
            logger.debug("(DefaultBusChannel({}@{})) sending message `{}`",
                channel.remoteAddress(), hashCode(), packetId);
            ChannelFuture writeResult = channel.writeAndFlush(outgoingMessage);

            if (writeResult.isDone()) {
                logWriteResult(packetId, started);
            } else {
                writeResult.addListener(unused -> logWriteResult(packetId, started));
            }

            BusUtil.relayResult(writeResult, result);
            BusUtil.relayCancel(result, writeResult);
        }
    }

    @Override
    public void channelConnected() {
        connected.trySuccess();
    }

    @Override
    public void channelDisconnected() {
        disconnected.trySuccess();
    }

    @Override
    public void channelFailed(Throwable cause) {
        connected.tryFailure(cause);
        disconnected.tryFailure(cause);
    }

    /**
     * Возвращает экземпляр объекта для данного канала
     * <p>
     * Создаёт и ассоциирует объект при первом вызове
     */
    public static DefaultBusChannel getOrCreateInstance(Channel channel) {
        Attribute<DefaultBusChannel> attr = channel.attr(CHANNEL_KEY);
        DefaultBusChannel bus = attr.get();
        if (bus == null) {
            bus = new DefaultBusChannel(channel);
            DefaultBusChannel old = attr.setIfAbsent(bus);
            if (old != null) {
                bus = old;
            }
        }
        return bus;
    }
}
