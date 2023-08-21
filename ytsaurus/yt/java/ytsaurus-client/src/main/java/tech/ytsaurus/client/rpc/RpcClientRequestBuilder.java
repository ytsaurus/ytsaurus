package tech.ytsaurus.client.rpc;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

import com.google.protobuf.MessageLite;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.rpc.TRequestHeader;

/**
 * Позволяет построить и в дальнейшем сделать запрос
 *
 * @param <RequestType>  Message.Builder
 * @param <ResponseType> RpcClientResponse&lt;Message&gt;
 */
public interface RpcClientRequestBuilder<RequestType extends MessageLite.Builder, ResponseType extends MessageLite> {
    /**
     * Мутабельное тело запроса
     */
    RequestType body();

    /**
     * Мутабельный список аттачей к запросу
     */
    List<byte[]> attachments();

    void setCompressedAttachments(Compression rpcCompression, List<byte[]> attachments);

    /**
     * Делает асинхронное выполнение запроса
     */
    CompletableFuture<RpcClientResponse<ResponseType>> invoke(RpcClient client);

    /**
     * Make asynchronous request. RpcClient is taken from given pool.
     */
    CompletableFuture<RpcClientResponse<ResponseType>> invokeVia(
            ScheduledExecutorService executor,
            RpcClientPool clientPool
    );

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
