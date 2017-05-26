package ru.yandex.yt.ytclient.bus;

/**
 * Интерфейс для установки соединений до фиксированного адресата
 */
public interface BusFactory {
    /**
     * Устанавливает новое соединение с указанным listener'ом
     */
    Bus createBus(BusListener listener);
}
