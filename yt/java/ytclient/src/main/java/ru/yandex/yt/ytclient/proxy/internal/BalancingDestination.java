package ru.yandex.yt.ytclient.proxy.internal;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

import ru.yandex.yt.rpcproxy.ETransactionType;
import ru.yandex.yt.ytclient.proxy.ApiServiceClient;
import ru.yandex.yt.ytclient.proxy.ApiServiceTransaction;
import ru.yandex.yt.ytclient.proxy.ApiServiceTransactionOptions;
import ru.yandex.yt.ytclient.rpc.RpcClient;
import ru.yandex.yt.ytclient.rpc.RpcClientRequestControl;
import ru.yandex.yt.ytclient.rpc.RpcClientResponseHandler;
import ru.yandex.yt.ytclient.rpc.RpcClientStreamControl;
import ru.yandex.yt.ytclient.rpc.RpcOptions;
import ru.yandex.yt.ytclient.rpc.RpcRequest;
import ru.yandex.yt.ytclient.rpc.RpcStreamConsumer;
import ru.yandex.yt.ytclient.rpc.internal.metrics.BalancingDestinationMetricsHolder;
import ru.yandex.yt.ytclient.rpc.internal.metrics.BalancingDestinationMetricsHolderImpl;

/**
 * @author aozeritsky
 */
public class BalancingDestination {
    private final BalancingDestinationMetricsHolder metricsHolder;

    private final String dc;
    private final RpcClient client;
    private final String id;

    private final ApiServiceClient service;
    private ApiServiceTransaction transaction = null;

    private final String destinationName;

    public BalancingDestination(String dc, RpcClient client, int index) {
        this(dc, client, new BalancingDestinationMetricsHolderImpl());
    }

    public BalancingDestination(String dc, RpcClient client, BalancingDestinationMetricsHolder metricsHolder) {
        this(dc, client, metricsHolder, new RpcOptions());
    }

    public BalancingDestination(String dc, RpcClient client, BalancingDestinationMetricsHolder metricsHolder, RpcOptions options) {
        this.dc = dc;
        this.client = Objects.requireNonNull(client);
        this.id = String.format("%s/%s", dc, client.toString());

        this.destinationName = client.destinationName();
        this.metricsHolder = metricsHolder;

        service = new ApiServiceClient(client, options);
    }

    /* for testing only */
    @Deprecated
    public BalancingDestination(String dc, int index) {
        this.dc = dc;
        BalancingDestination parent = this;
        this.client = new RpcClient() {
            @Override
            public void close() { }

            @Override
            public void ref() { }

            @Override
            public void unref() { }

            @Override
            public RpcClientRequestControl send(
                    RpcClient sender, RpcRequest<?> request,
                    RpcClientResponseHandler handler, RpcOptions options)
            {
                return null;
            }

            @Override
            public RpcClientStreamControl startStream(RpcClient sender, RpcRequest<?> request, RpcStreamConsumer consumer, RpcOptions options) {
                return null;
            }

            @Override
            public String destinationName() {
                return null;
            }

            @Override
            public String getAddressString() {
                return null;
            }

            @Override
            public ScheduledExecutorService executor() {
                return null;
            }

            @Override
            public String toString() {
                return parent.toString();
            }
        };
        this.id = String.format("%s/%d", dc, index);

        this.destinationName = "local";
        this.metricsHolder = new BalancingDestinationMetricsHolderImpl();

        service = null;
    }

    public double weight() {
        return metricsHolder.getLocal99thPercentile(destinationName);
    }

    public String dataCenter() {
        return dc;
    }

    public RpcClient getClient() {
        return client;
    }

    public ApiServiceClient getService() {
        return service;
    }

    public void close() {
        client.close();
    }

    CompletableFuture<ApiServiceTransaction> createTransaction(Duration timeout) {
        if (transaction == null) {
            return service.startTransaction(
                    new ApiServiceTransactionOptions(ETransactionType.TT_TABLET)
                        .setSticky(true)
                        .setTimeout(timeout.multipliedBy(2)));
        } else {
            return CompletableFuture.completedFuture(transaction);
        }
    }

    CompletableFuture<Void> pingTransaction(ApiServiceTransaction tx) {
        long start = System.nanoTime();

        return tx.ping().thenAccept(unused -> {
            transaction = tx;

            long end = System.nanoTime();
            long interval = (end - start) / 1000000;
            metricsHolder.updateLocal(destinationName, interval);
            metricsHolder.updateDc(dc, interval);
        });
    }

    void resetTransaction() {
        transaction = null;
    }

    @Override
    public String toString() {
        return id;
    }
}
