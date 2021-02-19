package ru.yandex.yt.ytclient.rpc;

import java.time.Duration;
import java.util.List;

import ru.yandex.inside.yt.kosher.common.GUID;
import ru.yandex.yt.rpc.TRequestHeader;

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
     * Возвращает id запроса
     */
    default GUID getRequestId() {
        return RpcUtil.fromProto(header().getRequestIdOrBuilder());
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
    default GUID getRealmId() {
        return RpcUtil.fromProto(header().getRealmIdOrBuilder());
    }

    /**
     * Возвращает id мутации
     */
    default GUID getMutationId() {
        return RpcUtil.fromProto(header().getMutationIdOrBuilder());
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

    RpcOptions getOptions();
}
