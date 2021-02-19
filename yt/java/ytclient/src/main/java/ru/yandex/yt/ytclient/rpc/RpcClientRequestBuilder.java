package ru.yandex.yt.ytclient.rpc;

import java.time.Duration;
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
public interface RpcClientRequestBuilder<RequestType extends MessageLite.Builder, ResponseType> extends RpcClientRequest {
    /**
     * Имя пользователя по умолчанию
     */
    String ROOT_USER_NAME = "root";

    /**
     * Мутабельное тело запроса
     */
    RequestType body();

    /**
     * Мутабельный список аттачей к запросу
     */
    List<byte[]> attachments();

    /**
     * Имя пользователя
     */
    default String getUser() {
        TRequestHeader.Builder header = header();
        return header.hasUser() ? header.getUser() : ROOT_USER_NAME;
    }

    /**
     * Выставляет имя пользователя
     */
    default void setUser(String user) {
        if (user.equals(ROOT_USER_NAME)) {
            header().clearUser();
        } else {
            header().setUser(user);
        }
    }

    /**
     * Возвращает true, если запрос является повторным
     */
    default boolean isRetry() {
        return header().getRetry();
    }

    /**
     * Помечает запрос как повторый, если retry == true
     */
    default void setRetry(boolean retry) {
        header().setRetry(retry);
    }

    /**
     * Выставляет id мутации
     */
    default void setMutationId(GUID mutationId) {
        if (mutationId.isEmpty()) {
            header().clearMutationId();
        } else {
            header().setMutationId(RpcUtil.toProto(mutationId));
        }
    }

    /**
     * Выставляет таймаут равным timeout
     */
    default void setTimeout(Duration timeout) {
        if (timeout == null) {
            header().clearTimeout();
        } else {
            header().setTimeout(RpcUtil.durationToMicros(timeout));
        }
    }

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

}
