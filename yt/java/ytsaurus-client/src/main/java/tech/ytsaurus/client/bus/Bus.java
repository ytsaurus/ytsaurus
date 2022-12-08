package tech.ytsaurus.client.bus;

import java.net.SocketAddress;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoop;

/**
 * Интерфейс для передачи сообщений по протоколу bus
 */
public interface Bus {
    /**
     * Возвращает netty канал соединения
     */
    Channel channel();

    /**
     * Возвращает event loop в котором работает соединение
     */
    EventLoop eventLoop();

    /**
     * Возвращает ChannelFuture, сигнализирующий о завершении коннекта
     */
    ChannelFuture connected();

    /**
     * Возвращает ChannelFuture, сигнализирующий о завершении соединения
     */
    ChannelFuture disconnected();

    /**
     * Возвращает ChannelFuture, сигнализирующий о закрытии соединения
     */
    ChannelFuture closed();

    /**
     * Возвращает локальный адрес соединения
     */
    SocketAddress localAddress();

    /**
     * Возвращает удалённый адрес соединения
     */
    SocketAddress remoteAddress();

    /**
     * Инициирует закрытие соединения
     */
    CompletableFuture<Void> close();

    /**
     * Инициирует отправку сообщения с указанным уровнем слежения за доставкой
     * <p>
     * В случае вызова CompletableFuture.cancel отправка сообщения может быть отменена
     */
    CompletableFuture<Void> send(List<byte[]> message, BusDeliveryTracking level);
}
