package ru.yandex.yt.ytclient.rpc;

import java.util.List;

/**
 * Интерфейс для низкоуровневой обработки жизненного цикла запроса
 * <p>
 * Методы могут вызываться из io потока, любые блокировки нежелательны
 */
public interface RpcClientResponseHandler {
    /**
     * Вызывается после получения подтверждения о доставке
     * <p>
     * Вызов метода не гарантируется, может быть вызван параллельно с onResponse/onError
     */
    void onAcknowledgement(RpcClient sender);

    /**
     * Вызывается при получении сырого успешного ответа
     */
    void onResponse(RpcClient sender, List<byte[]> attachments);

    /**
     * Вызывается в случае ошибок
     */
    void onError(Throwable error);
}
