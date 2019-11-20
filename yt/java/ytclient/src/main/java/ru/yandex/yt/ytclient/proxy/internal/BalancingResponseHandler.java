package ru.yandex.yt.ytclient.proxy.internal;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import ru.yandex.bolts.collection.Tuple3;
import ru.yandex.yt.rpc.TResponseHeader;
import ru.yandex.yt.ytclient.rpc.RpcClient;
import ru.yandex.yt.ytclient.rpc.RpcClientRequest;
import ru.yandex.yt.ytclient.rpc.RpcClientRequestControl;
import ru.yandex.yt.ytclient.rpc.RpcClientResponseHandler;
import ru.yandex.yt.ytclient.rpc.RpcFailoverPolicy;
import ru.yandex.yt.ytclient.rpc.internal.metrics.BalancingResponseHandlerMetricsHolder;

/**
 * @author aozeritsky
 */
public class BalancingResponseHandler implements RpcClientResponseHandler {
    private final BalancingResponseHandlerMetricsHolder metricsHolder;

    private final CompletableFuture<Tuple3<RpcClient, TResponseHeader, List<byte[]>>> f;
    private List<RpcClient> clients;
    private final RpcClientRequest request;
    private int step;
    private final List<RpcClientRequestControl> cancelation;
    final private ScheduledExecutorService executorService;
    final private RpcFailoverPolicy failoverPolicy;

    final private long failoverTimeout;
    private long timeout;

    public BalancingResponseHandler(
            ScheduledExecutorService executorService,
            CompletableFuture<Tuple3<RpcClient, TResponseHeader, List<byte[]>>> f,
            RpcClientRequest request,
            List<RpcClient> clients)
    {
        this.executorService = executorService;
        this.failoverPolicy = request.getOptions().getFailoverPolicy();
        this.failoverTimeout = request.getOptions().getFailoverTimeout().toMillis();

        this.f = f;
        this.request = request;
        this.clients = clients;
        this.metricsHolder = request.getOptions().getResponseMetricsHolder();

        timeout = request.getOptions().getGlobalTimeout().toMillis();

        cancelation = new ArrayList<>();
        step = 0;

        if (clients.isEmpty()) {
            f.completeExceptionally(new RuntimeException("empty destinations list"));
        } else {
            send();
        }
    }

    private void send() {
        if (timeout <= 0) {
            f.completeExceptionally(new RuntimeException("request timeout"));
            return;
        }

        RpcClient client = clients.get(0);
        clients = clients.subList(1, clients.size());

        if (step > 0) {
           metricsHolder.failoverInc();
        }

        step ++;

        metricsHolder.inflightInc();
        metricsHolder.totalInc();

        request.header().setTimeout(timeout*1000); // in microseconds
        cancelation.add(client.send(request, this));

        // schedule next step
        executorService.schedule(() ->
                onTimeout(), failoverTimeout, TimeUnit.MILLISECONDS);

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
                    f.completeExceptionally(new RuntimeException("timeout"));
                }
            }
        }
    }

    public void cancel() {
        synchronized (f) {
            for (RpcClientRequestControl control : cancelation) {
                metricsHolder.inflightDec();
                control.cancel();
            }
        }
    }

    @Override
    public void onAcknowledgement(RpcClient sender) { }

    @Override
    public void onResponse(RpcClient sender, TResponseHeader header, List<byte[]> attachments) {
        f.complete(new Tuple3<>(sender, header, attachments));
    }

    @Override
    public void onError(Throwable error) {
        synchronized (f) {
            if (!f.isDone()) {
                // maybe use other proxy here?
                if (failoverPolicy.onError(request, error) && !clients.isEmpty()) {
                    send();
                } else {
                    f.completeExceptionally(error);
                }
            }
        }
    }
}
