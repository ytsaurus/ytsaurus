package ru.yandex.yt.ytclient.proxy;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;
import ru.yandex.yt.rpc.TReqDiscover;
import ru.yandex.yt.rpc.TRspDiscover;
import ru.yandex.yt.ytclient.misc.ScheduledSerializedExecutorService;
import ru.yandex.yt.ytclient.proxy.internal.FailureDetectingRpcClient;
import ru.yandex.yt.ytclient.proxy.internal.HostPort;
import ru.yandex.yt.ytclient.proxy.internal.RpcClientFactory;
import ru.yandex.yt.ytclient.rpc.RpcClient;
import ru.yandex.yt.ytclient.rpc.RpcClientRequestBuilder;
import ru.yandex.yt.ytclient.rpc.RpcClientResponse;
import ru.yandex.yt.ytclient.rpc.RpcError;
import ru.yandex.yt.ytclient.rpc.RpcErrorCode;
import ru.yandex.yt.ytclient.rpc.RpcOptions;

public interface SelfCheckingClientFactory {
    RpcClient create(HostPort hostPort, String name, CompletableFuture<Void> statusFuture);
}

@NonNullApi
@NonNullFields
class SelfCheckingClientFactoryImpl implements SelfCheckingClientFactory {
    RpcClientFactory underlying;
    RpcOptions options;

    SelfCheckingClientFactoryImpl(RpcClientFactory underlying, RpcOptions options) {
        this.underlying = underlying;
        this.options = options;
    }

    @Override
    public RpcClient create(HostPort hostPort, String name, CompletableFuture<Void> statusFuture) {
        RpcClient client = underlying.create(hostPort, name);
        return new SelfCheckingClient(client, options, statusFuture);
    }
}

@NonNullApi
@NonNullFields
class SelfCheckingClient extends FailureDetectingRpcClient implements RpcClient {
    private static final Logger logger = LoggerFactory.getLogger(ClientPool.class);

    private static final Duration PING_PERIOD = Duration.ofSeconds(5);
    private static final Duration PING_TIMEOUT = Duration.ofSeconds(5);

    final ScheduledExecutorService executorService;
    final RpcOptions options;

    final CompletableFuture<Void> statusFuture;
    volatile CompletableFuture<RpcClientResponse<TRspDiscover>> pingResult = CompletableFuture.completedFuture(null);

    SelfCheckingClient(RpcClient innerClient, RpcOptions options, CompletableFuture<Void> statusFuture) {
        super(innerClient);
        this.statusFuture = statusFuture;
        setHandlers(
                e -> {
                    if (e instanceof RpcError && ((RpcError) e).matches(RpcErrorCode.TableMountInfoNotReady.code)) {
                        // Workaround: we want to treat such errors as recoverable.
                        return false;
                    }
                    return RpcError.isUnrecoverable(e);
                },
                e -> {
                    logger.debug("Self checking client {} detected fatal error:", this, e);
                    statusFuture.completeExceptionally(e);
                });
        executorService = new ScheduledSerializedExecutorService(innerClient.executor());

        this.options = new RpcOptions();
        this.options.setDefaultRequestAck(options.getDefaultRequestAck());
        this.options.setGlobalTimeout(PING_TIMEOUT);

        executorService.submit(this::scheduleNextPing);
    }

    void scheduleNextPing() {
        pingResult.whenComplete((result, error) -> {
            if (error != null) {
                logger.debug("Self checking client {} detected ping error: ", this, error);
                statusFuture.completeExceptionally(error);
            } else if (result != null && !result.body().getUp()) {
                logger.debug("Self checking client {} detected proxy is down", this);
                statusFuture.completeExceptionally(new RuntimeException("Proxy is down"));
            } else {
                executorService.schedule(
                        () -> {
                            RpcClientRequestBuilder<TReqDiscover.Builder, TRspDiscover> requestBuilder =
                                    ApiServiceMethodTable.DISCOVER.createRequestBuilder(this.options);

                            pingResult = requestBuilder.invoke(this);
                            scheduleNextPing();
                        },
                        PING_PERIOD.toMillis(),
                        TimeUnit.MILLISECONDS);
            }
        });
    }
}
