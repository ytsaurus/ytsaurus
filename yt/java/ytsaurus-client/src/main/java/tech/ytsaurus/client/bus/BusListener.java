package tech.ytsaurus.client.bus;

import java.util.List;


/**
 * Обработчик жизненного цикла bus соединений
 * <p>
 * Методы вызываются из io потока, любые блокировки нежелательны
 */
public interface BusListener {
    /**
     * Вызывается при поступлении bus сообщения
     */
    void onMessage(Bus bus, List<byte[]> message);

    /**
     * Вызывается при подключении соединения
     */
    void onConnect(Bus bus);

    /**
     * Вызывается при отключении соединения
     */
    void onDisconnect(Bus bus);

    /**
     * Вызывается, если обработка соединения завершается с ошибкой
     */
    void onException(Bus bus, Throwable cause);
}
