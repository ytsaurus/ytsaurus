package ru.yandex.yt.ytclient.rpc;

import java.time.Duration;
import java.util.List;

import ru.yandex.yt.rpc.TRequestHeader;
import ru.yandex.yt.ytclient.misc.YtGuid;

/**
 * Базовый интерфейс для построения rpc запросов
 * <p>
 * Внимание: не является потоко-безопасным
 */
public interface RpcClientRequest {
    /**
     * Возвращает мутабельный заголовок запроса, который можно менять перед отправкой
     */
    TRequestHeader.Builder header();

    /**
     * Сериализует тело запроса в bus сообщение
     */
    List<byte[]> serialize();

    /**
     * Возвращает true, если от запроса требуется подтверждение о доставке
     */
    boolean requestAck();

    /**
     * Возвращает true, если от запроса не ожидается ответного сообщения
     */
    default boolean isOneWay() {
        // TODO: remove one way support code
        return false;
    }

    /**
     * Возвращает id запроса
     */
    default YtGuid getRequestId() {
        return YtGuid.fromProto(header().getRequestIdOrBuilder());
    }

    /**
     * Имя сервиса к которому будет делаться запрос
     */
    default String getService() {
        return header().getService();
    }

    /**
     * Имя метода к которому будет делаться запрос
     */
    default String getMethod() {
        return header().getMethod();
    }

    /**
     * Возвращает id области
     */
    default YtGuid getRealmId() {
        return YtGuid.fromProto(header().getRealmIdOrBuilder());
    }

    /**
     * Возвращает id мутации
     */
    default YtGuid getMutationId() {
        return YtGuid.fromProto(header().getMutationIdOrBuilder());
    }

    /**
     * Возвращает timeout, в течение которого должен быть получен ответ на запрос
     */
    default Duration getTimeout() {
        TRequestHeader.Builder header = header();
        if (header.hasTimeout()) {
            return RpcUtil.durationFromMicros(header.getTimeout());
        } else {
            return null;
        }
    }
}
