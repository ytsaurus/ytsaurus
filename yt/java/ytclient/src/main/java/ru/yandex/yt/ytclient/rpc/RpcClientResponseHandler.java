package ru.yandex.yt.ytclient.rpc;

import java.util.List;
import java.util.concurrent.CancellationException;

import ru.yandex.yt.rpc.TResponseHeader;

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
    void onResponse(RpcClient sender, TResponseHeader header, List<byte[]> attachments);

    /**
     * Вызывается в случае ошибок
     */
    void onError(Throwable error);

    /**
     * Вызывается в случае отмены запроса
     */
    void onCancel(CancellationException cancel);
}
