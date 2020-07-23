package ru.yandex.yt.ytclient.proxy.internal;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;

import ru.yandex.bolts.collection.Tuple3;
import ru.yandex.yt.rpc.TResponseHeader;
import ru.yandex.yt.ytclient.rpc.RpcClient;
import ru.yandex.yt.ytclient.rpc.RpcClientRequest;
import ru.yandex.yt.ytclient.rpc.RpcClientRequestControl;
import ru.yandex.yt.ytclient.rpc.RpcClientResponseHandler;
import ru.yandex.yt.ytclient.rpc.RpcFailoverPolicy;
import ru.yandex.yt.ytclient.rpc.internal.metrics.BalancingResponseHandlerMetricsHolder;

public class FailoverRpcExecutor {
    private final ScheduledExecutorService executorService;
    private final BalancingResponseHandlerMetricsHolder metricsHolder;
    private List<RpcClient> clients;
    private final List<RpcClientRequestControl> cancellation;
    private final RpcFailoverPolicy failoverPolicy;
    private final long failoverTimeout;

    private final RpcClientRequest request;
    private final RpcClientResponseHandler baseHandler;

    private final CompletableFuture<Tuple3<RpcClient, TResponseHeader, List<byte[]>>> result;


    private long timeout;
    private int step;

    public FailoverRpcExecutor(ScheduledExecutorService executorService,
                               List<RpcClient> clients,
                               RpcClientRequest request,
                               RpcClientResponseHandler handler) {
        this.executorService = executorService;
        this.clients = clients;
        this.timeout = request.getOptions().getGlobalTimeout().toMillis();
        this.step = 0;
        this.metricsHolder = request.getOptions().getResponseMetricsHolder();
        this.cancellation = new ArrayList<>();
        this.failoverPolicy = request.getOptions().getFailoverPolicy();
        this.failoverTimeout = request.getOptions().getFailoverTimeout().toMillis();

        this.request = request;
        this.baseHandler = handler;

        this.result = new CompletableFuture<>();
    }

    private static class FailoverResponseHandler implements RpcClientResponseHandler {
        private final Predicate<Throwable> resendOnError;
        private final Supplier<Boolean> resendOnTimeout;
        private final Consumer<FailoverResponseHandler> resend;
        private final CompletableFuture<Tuple3<RpcClient, TResponseHeader, List<byte[]>>> result;

        FailoverResponseHandler(CompletableFuture<Tuple3<RpcClient, TResponseHeader, List<byte[]>>> result,
                                       Predicate<Throwable> resendOnError,
                                       Supplier<Boolean> resendOnTimeout,
                                       Consumer<FailoverResponseHandler> resend) {
            this.resendOnError = resendOnError;
            this.resendOnTimeout = resendOnTimeout;
            this.resend = resend;
            this.result = result;
        }

        @Override
        public void onAcknowledgement(RpcClient sender) {

        }

        @Override
        public void onResponse(RpcClient sender, TResponseHeader header, List<byte[]> attachments) {
            synchronized (result) {
                if (!result.isDone()) {
                    result.complete(new Tuple3<>(sender, header, attachments));
                }
            }
        }

        @Override
        public void onError(Throwable error) {
            synchronized (result) {
                if (!result.isDone()) {
                    if (resendOnError.test(error)) {
                        resend.accept(this);
                    } else {
                        result.completeExceptionally(error);
                    }
                }
            }
        }

        @Override
        public void onCancel(CancellationException cancel) {
            synchronized (result) {
                if (!result.isDone()) {
                    result.completeExceptionally(cancel);
                }
            }
        }

        public void onGlobalTimeout() {
            synchronized (result) {
                if (!result.isDone()) {
                    result.completeExceptionally(new TimeoutException("Request timeout"));
                }
            }
        }

        public void onTimeout() {
            synchronized (result) {
                if (!result.isDone()) {
                    if (resendOnTimeout.get()) {
                        resend.accept(this);
                    }
                }
            }
        }
    }


    private void execute(FailoverResponseHandler handler) {
        executorService.schedule(handler::onGlobalTimeout, timeout, TimeUnit.MILLISECONDS);
        send(handler);
    }

    private void send(FailoverResponseHandler handler) {
        if (timeout <= 0) {
            handler.onGlobalTimeout();
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
        cancellation.add(client.send(request, handler));

        // schedule next step
        executorService.schedule(handler::onTimeout, failoverTimeout, TimeUnit.MILLISECONDS);

        timeout -= failoverTimeout;
    }


    private Boolean resendOnTimeout() {
        return !clients.isEmpty() && failoverPolicy.onTimeout();
    }

    private Boolean resendOnError(Throwable error) {
        return failoverPolicy.onError(request, error) && !clients.isEmpty();

    }

    private void handleResult(Tuple3<RpcClient, TResponseHeader, List<byte[]>> result,
                              Throwable error) {
        if (error == null) {
            baseHandler.onResponse(result.get1(), result.get2(), result.get3());
        } else {
            baseHandler.onError(error);
        }
    }

    private void cancel() {
        synchronized (result) {
            for (RpcClientRequestControl control : cancellation) {
                metricsHolder.inflightDec();
                control.cancel();
            }
        }
    }

    public RpcClientRequestControl execute() {
        CompletableFuture<Tuple3<RpcClient, TResponseHeader, List<byte[]>>> f = new CompletableFuture<>();
        try {
            FailoverResponseHandler failoverResponseHandler = new FailoverResponseHandler(
                    f, this::resendOnError, this::resendOnTimeout, this::send);

            if (clients.isEmpty()) {
                f.completeExceptionally(new RuntimeException("Empty destinations list"));
            } else {
                execute(failoverResponseHandler);
            }

            f.whenComplete((result, error) -> {
                cancel();
                handleResult(result, error);
            });
        } catch (Throwable e) {
            baseHandler.onError(e);
        }

        return () -> f.cancel(true);
    }
}
