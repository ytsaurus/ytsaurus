package ru.yandex.yt.ytclient.rpc.internal;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;

import ru.yandex.yt.ytclient.rpc.BalancingRpcClient;
import ru.yandex.yt.ytclient.rpc.RpcClient;
import ru.yandex.yt.ytclient.rpc.RpcClientRequest;
import ru.yandex.yt.ytclient.rpc.RpcClientRequestControl;
import ru.yandex.yt.ytclient.rpc.RpcClientResponseHandler;
import ru.yandex.yt.ytclient.rpc.RpcFailoverPolicy;

/**
 * @author aozeritsky
 */
public class BalancingResponseHandler implements RpcClientResponseHandler {
    // TODO: move somewhere to core
    private static final MetricRegistry metrics = SharedMetricRegistries.getOrCreate("ytclient");
    private static final Counter inflight = metrics.counter(MetricRegistry.name(BalancingRpcClient.class, "requests", "inflight"));
    private static final Counter failover = metrics.counter(MetricRegistry.name(BalancingRpcClient.class,"requests", "failover"));
    private static final Counter total = metrics.counter(MetricRegistry.name(BalancingRpcClient.class,"requests", "total"));

    private final CompletableFuture<List<byte[]>> f;
    private List<BalancingDestination> clients;
    private final RpcClientRequest request;
    private final boolean isOneWay;
    private int step;
    private final List<RpcClientRequestControl> cancelation;
    private Future<?> timeoutFuture;
    final private ScheduledExecutorService executorService;
    final private RpcFailoverPolicy failoverPolicy;

    final private long failoverTimeout;
    private long timeout;

    public BalancingResponseHandler(
        ScheduledExecutorService executorService,
        RpcFailoverPolicy failoverPolicy,
        Duration globalTimeout,
        Duration failoverTimeout,
        CompletableFuture<List<byte[]>> f,
        RpcClientRequest request,
        List<BalancingDestination> clients)
    {
        this.executorService = executorService;
        this.failoverPolicy = failoverPolicy;
        this.failoverTimeout = failoverTimeout.toMillis();

        this.f = f;
        this.request = request;
        this.isOneWay = request.isOneWay();
        this.clients = clients;

        timeout = globalTimeout.toMillis();

        cancelation = new ArrayList<>();
        timeoutFuture = CompletableFuture.completedFuture(0);
        step = 0;
        send();
    }

    private void send() {
        if (timeout <= 0) {
            f.completeExceptionally(new RuntimeException("request timeout"));
            return;
        }

        RpcClient client;
        BalancingDestination dst = clients.get(0);
        clients = clients.subList(1, clients.size());
        client = dst.getClient();

        if (step > 0) {
            failover.inc();
        }

        step ++;

        inflight.inc();
        total.inc();

        request.header().setTimeout(timeout*1000); // in microseconds
        cancelation.add(client.send(request, this));

        // schedule next step
        executorService.schedule(() ->
                onTimeout()
            , step * failoverTimeout, TimeUnit.MILLISECONDS);

        timeout -= failoverTimeout;
    }

    private void onTimeout() {
        synchronized (f) {
            if (!f.isDone()) {
                if (!clients.isEmpty()){
                    if (failoverPolicy.onTimeout()) {
                        send();
                    } else {
                        f.completeExceptionally(new RuntimeException("timeout"));
                    }
                } else {
                    // global timeout
                }
            }
        }
    }

    public void cancel() {
        synchronized (f) {
            for (RpcClientRequestControl control : cancelation) {
                inflight.dec();
                control.cancel();
            }
        }
    }

    @Override
    public void onAcknowledgement() {
        if (isOneWay) {
            synchronized (f) {
                if (!f.isDone()) {
                    f.complete(null);
                }
            }
        }
    }

    @Override
    public void onResponse(List<byte[]> attachments) {
        synchronized (f) {
            if (!f.isDone()) {
                if (isOneWay) {
                    // FATAL error, cannot recover
                    f.completeExceptionally(new IllegalStateException("Server replied to a one-way request"));
                } else {
                    f.complete(attachments);
                }
            }
        }
    }

    @Override
    public void onError(Throwable error) {
        synchronized (f) {
            if (!f.isDone()) {
                // maybe use other proxy here?
                if (failoverPolicy.onError(request, error) && !clients.isEmpty()) {
                    timeoutFuture.cancel(true);
                    send();
                } else {
                    f.completeExceptionally(error);
                }
            }
        }
    }
}
