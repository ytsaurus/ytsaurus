package tech.ytsaurus.client.rpc;

import java.util.concurrent.ScheduledExecutorService;

import javax.annotation.Nullable;

/**
 * Клиент через который можно делать запросы и получать на них ответы
 */
public interface RpcClient extends AutoCloseable {
    /**
     * Reference counting to use with ClientPool.
     *
     * <p><b>Not to be used by library clients.</b>
     * </p>
     *
     * <p>RpcClient must be created with reference counter set to 1.
     * Once reference counter reaches 0, {@link #close()} is called.
     *
     * @see #unref()
     */
    void ref();

    /**
     * @see #ref()
     */
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
    RpcClientRequestControl send(
            RpcClient sender,
            RpcRequest<?> request,
            RpcClientResponseHandler handler,
            RpcOptions options
    );

    RpcClientStreamControl startStream(
            RpcClient sender,
            RpcRequest<?> request,
            RpcStreamConsumer consumer,
            RpcOptions options
    );

    default RpcClient withAuthentication(RpcCredentials credentials) {
        return new AuthenticationWrapper(this, credentials);
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
