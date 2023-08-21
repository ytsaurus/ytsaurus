package tech.ytsaurus.client.rpc;

import java.util.List;

/**
 * Тело ответа с
 *
 * @param <ResponseType>
 */
public interface RpcClientResponse<ResponseType> {
    /**
     * Тело ответа
     */
    ResponseType body();

    /**
     * Приаттаченные к ответу данные
     */
    List<byte[]> attachments();

    RpcClient sender();
}
