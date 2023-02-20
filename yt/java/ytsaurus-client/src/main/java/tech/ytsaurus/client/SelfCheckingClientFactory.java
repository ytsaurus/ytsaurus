package tech.ytsaurus.client;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ytsaurus.client.misc.ScheduledSerializedExecutorService;
import tech.ytsaurus.client.rpc.Compression;
import tech.ytsaurus.client.rpc.RpcClient;
import tech.ytsaurus.client.rpc.RpcClientRequestBuilder;
import tech.ytsaurus.client.rpc.RpcClientResponse;
import tech.ytsaurus.client.rpc.RpcError;
import tech.ytsaurus.client.rpc.RpcErrorCode;
import tech.ytsaurus.client.rpc.RpcOptions;
import tech.ytsaurus.lang.NonNullApi;
import tech.ytsaurus.lang.NonNullFields;
import tech.ytsaurus.rpc.TReqDiscover;
import tech.ytsaurus.rpc.TRspDiscover;


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
                            requestBuilder.header().setRequestCodec(Compression.None.getValue());
                            requestBuilder.header().setResponseCodec(Compression.None.getValue());

                            pingResult = requestBuilder.invoke(this);
                            scheduleNextPing();
                        },
                        PING_PERIOD.toMillis(),
                        TimeUnit.MILLISECONDS);
            }
        });
    }
}
