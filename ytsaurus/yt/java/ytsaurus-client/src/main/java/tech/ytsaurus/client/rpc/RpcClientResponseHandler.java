package tech.ytsaurus.client.rpc;

import java.util.List;
import java.util.concurrent.CancellationException;

import tech.ytsaurus.rpc.TResponseHeader;

/**
 * Интерфейс для низкоуровневой обработки жизненного цикла запроса
 * <p>
 * Методы могут вызываться из io потока, любые блокировки нежелательны
 */
public interface RpcClientResponseHandler {
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
