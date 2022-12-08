package tech.ytsaurus.client.bus;

import java.net.SocketAddress;
import java.util.concurrent.ScheduledExecutorService;

import io.netty.channel.EventLoopGroup;

/**
 * Интерфейс для установки соединений по протоколу bus
 */
public interface BusConnector extends AutoCloseable {
    /**
     * Устанавливает новое соединение с address и указанным listener
     * <p>
     * В случае успеха возвращает шину сообщений
     * <p>
     * В случае вызова CompletableFuture.cancel операция connect'а может быть отменена
     */
    Bus connect(SocketAddress address, BusListener listener);

    /**
     * Начинает принимать bus соединения по адресу address
     */
    BusServer listen(SocketAddress address, BusListener listener);

    EventLoopGroup eventLoopGroup();

    default ScheduledExecutorService executorService() {
        return eventLoopGroup();
    }

    @Override
    void close();
}
