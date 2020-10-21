package ru.yandex.yt.ytclient.rpc;

import java.util.concurrent.ScheduledExecutorService;

import javax.annotation.Nullable;

import ru.yandex.yt.ytclient.rpc.internal.RpcClientWithCompression;
import ru.yandex.yt.ytclient.rpc.internal.TokenAuthentication;

/**
 * Клиент через который можно делать запросы и получать на них ответы
 */
public interface RpcClient extends AutoCloseable {
    void ref();
    void unref();

    /**
     * Асинхронно закрывает rpc клиент
     */
    @Override
    void close();

    /**
     * Отправляет запрос, который можно отменить через возвращённый CompletableFuture
     * <p>
     * Сериализация запроса происходит в текущем потоке
     */
    RpcClientRequestControl send(RpcClient sender, RpcClientRequest request, RpcClientResponseHandler handler);

    default
    RpcClientRequestControl send(RpcClientRequest request, RpcClientResponseHandler handler)
    {
        return send(this, request, handler);
    }

    RpcClientStreamControl startStream(RpcClient sender, RpcClientRequest request, RpcStreamConsumer consumer);

    default RpcClientStreamControl startStream(RpcClientRequest request, RpcStreamConsumer consumer) {
        return startStream(this, request, consumer);
    }

    /**
     * Возвращает клиент с аутентификацией запросов по токену
     * @deprecated {@see #withTokenAuthentication}
     */
    @Deprecated
    default RpcClient withTokenAuthentication(String user, String token) {
        return withTokenAuthentication(new RpcCredentials(user, token));
    }

    default RpcClient withTokenAuthentication(RpcCredentials credentials) {
        return new TokenAuthentication(this, credentials);
    }

    default RpcClient withCompression(RpcCompression compression) {
        return new RpcClientWithCompression(this, compression);
    }

    @Deprecated
    String destinationName();

    /**
     * Get address string of proxy that client is connected to if known.
     * Might be null, if proxy is not known for some reason (e.g. client wraps several connections).
     */
    @Nullable
    String getAddressString();

    ScheduledExecutorService executor();
}
