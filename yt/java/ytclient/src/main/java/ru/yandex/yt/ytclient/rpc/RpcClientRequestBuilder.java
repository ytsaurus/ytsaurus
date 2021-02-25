package ru.yandex.yt.ytclient.rpc;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

import com.google.protobuf.MessageLite;

import ru.yandex.inside.yt.kosher.common.GUID;
import ru.yandex.yt.rpc.TRequestHeader;

/**
 * Позволяет построить и в дальнейшем сделать запрос
 *
 * @param <RequestType>  Message.Builder
 * @param <ResponseType> Void или RpcClientResponse&lt;Message&gt;
 */
public interface RpcClientRequestBuilder<RequestType extends MessageLite.Builder, ResponseType> {
    /**
     * Мутабельное тело запроса
     */
    RequestType body();

    /**
     * Мутабельный список аттачей к запросу
     */
    List<byte[]> attachments();

    /**
     * Делает асинхронное выполнение запроса
     */
    CompletableFuture<ResponseType> invoke(RpcClient client);

    /**
     * Make asynchronous request. RpcClient is taken from given pool.
     */
    CompletableFuture<ResponseType> invokeVia(ScheduledExecutorService executor, RpcClientPool clientPool);

    RpcClientStreamControl startStream(RpcClient client, RpcStreamConsumer consumer);

    CompletableFuture<RpcClientStreamControl> startStream(
            ScheduledExecutorService executor,
            RpcClientPool clientPool,
            RpcStreamConsumer consumer);

    RpcOptions getOptions();

    /**
     * Возвращает мутабельный заголовок запроса, который можно менять перед отправкой
     */
    TRequestHeader.Builder header();

    RpcRequest<?> getRpcRequest();

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
}
