package tech.ytsaurus.client.bus;

import java.net.SocketAddress;
import java.util.concurrent.CompletableFuture;

import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoop;
import io.netty.channel.ServerChannel;

/**
 * Интерфейс для серверного соединения bus
 * <p>
 * Пока используется только в тестах bus.
 */
public interface BusServer {
    /**
     * Возвращает netty канал сервера
     */
    ServerChannel channel();

    /**
     * Возвращает event loop в котором работает сервер
     */
    EventLoop eventLoop();

    /**
     * Возвращает ChannelFuture, сигнализирующий о завершении операции bind
     */
    ChannelFuture bound();

    /**
     * Возвращает ChannelFuture, сигнализирующий о закрытии сервера
     */
    ChannelFuture closed();

    /**
     * Возвращает локальный адрес сервера
     */
    SocketAddress localAddress();

    /**
     * Инициирует закрытие сервера
     */
    CompletableFuture<Void> close();
}
