package ru.yandex.yt.ytclient.proxy.internal;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.yandex.yt.rpc.TResponseHeader;
import ru.yandex.yt.ytclient.misc.ScheduledSerializedExecutorService;
import ru.yandex.yt.ytclient.rpc.RpcClient;
import ru.yandex.yt.ytclient.rpc.RpcClientPool;
import ru.yandex.yt.ytclient.rpc.RpcClientRequest;
import ru.yandex.yt.ytclient.rpc.RpcClientRequestControl;
import ru.yandex.yt.ytclient.rpc.RpcClientResponseHandler;
import ru.yandex.yt.ytclient.rpc.RpcFailoverPolicy;
import ru.yandex.yt.ytclient.rpc.internal.metrics.BalancingResponseHandlerMetricsHolder;

public class FailoverRpcExecutor {
    private static final Logger logger = LoggerFactory.getLogger(FailoverRpcExecutor.class);

    private final ScheduledSerializedExecutorService serializedExecutorService;
    private final BalancingResponseHandlerMetricsHolder metricsHolder;
    private final RpcClientPool clientPool;
    private final RpcFailoverPolicy failoverPolicy;
    private final long failoverTimeout;
    private final long globalDeadline;

    private final RpcClientRequest request;
    private final RpcClientResponseHandler baseHandler;

    private final CompletableFuture<Result> result = new CompletableFuture<>();
    private final int attemptCount;

    private final MutableState mutableState;

    static public RpcClientRequestControl execute(
            ScheduledExecutorService executorService,
            RpcClientPool clientPool,
            RpcClientRequest request,
            RpcClientResponseHandler handler,
            int attemptCount)
    {
        return new FailoverRpcExecutor(executorService, clientPool, request, handler, attemptCount)
                .execute();
    }

    private FailoverRpcExecutor(
            ScheduledExecutorService executorService,
            RpcClientPool clientPool,
            RpcClientRequest request,
            RpcClientResponseHandler handler,
            int attemptCount)
    {
        this.serializedExecutorService = new ScheduledSerializedExecutorService(executorService);
        this.clientPool = clientPool;
        this.metricsHolder = request.getOptions().getResponseMetricsHolder();
        this.failoverPolicy = request.getOptions().getFailoverPolicy();
        this.failoverTimeout = request.getOptions().getFailoverTimeout().toMillis();
        this.globalDeadline = System.currentTimeMillis() + request.getOptions().getGlobalTimeout().toMillis();
        this.attemptCount = attemptCount;

        this.request = request;
        this.baseHandler = handler;

        this.mutableState = new MutableState();
    }

    private RpcClientRequestControl execute() {
        FailoverResponseHandler failoverResponseHandler = new FailoverResponseHandler();

        long globalDelay = globalDeadline - System.currentTimeMillis();
        serializedExecutorService.schedule(
                this::onGlobalTimeout,
                globalDelay,
                TimeUnit.MILLISECONDS);
        send(failoverResponseHandler);

        result.whenComplete((result, error) -> {
            serializedExecutorService.submit(mutableState::cancel);
            handleResult(result, error);
        });

        return () -> result.cancel(true);
    }

    private void send(FailoverResponseHandler handler) {
        clientPool.peekClient(result).whenCompleteAsync((RpcClient client, Throwable error) -> {
            if (error == null) {
                mutableState.sendImpl(client, handler);
                return;
            }

            logger.warn("Failed to get RpcClient from pool", error);
            mutableState.softAbort(error);
        }, serializedExecutorService);
    }

    private void handleResult(Result result, Throwable error) {
        if (error == null) {
            baseHandler.onResponse(result.client, result.header, result.data);
        } else {
            baseHandler.onError(error);
        }
    }

    private void onGlobalTimeout() {
        result.completeExceptionally(new TimeoutException("Request timeout"));
    }

    static private class Result {
        final RpcClient client;
        final TResponseHeader header;
        final List<byte[]> data;

        Result(RpcClient client, TResponseHeader header, List<byte[]> data) {
            this.client = client;
            this.header = header;
            this.data = data;
        }
    }

    private class FailoverResponseHandler implements RpcClientResponseHandler {
        @Override
        public void onAcknowledgement(RpcClient sender) {
        }

        @Override
        public void onResponse(RpcClient sender, TResponseHeader header, List<byte[]> attachments) {
            result.complete(new Result(sender, header, attachments));
        }

        @Override
        public void onError(Throwable error) {
            serializedExecutorService.submit(() -> mutableState.onRequestError(error, this));
        }

        @Override
        public void onCancel(CancellationException cancel) {
            result.completeExceptionally(cancel);
        }
    }

    // All state of our request that is not thread safe is inside this class.
    // All methods of this class MUST be called inside our serializedExecutorService
    private class MutableState {
        private final List<RpcClientRequestControl> cancellation = new ArrayList<>();

        private int requestsSent = 0;
        private int requestsError = 0;
        private boolean stopped = false;
        private Throwable lastRequestError = null;

        // If all requests that were sent already have failed then complete our result future with error result.
        // Otherwise no other retry will be performed, if we get
        public void softAbort(Throwable error) {
            stopped = true;
            if (requestsError == requestsSent) {
                if (lastRequestError == null) {
                    result.completeExceptionally(error);
                } else {
                    result.completeExceptionally(lastRequestError);
                }
            }
        }

        public void onRequestError(Throwable error, FailoverResponseHandler handler) {
            requestsError++;
            lastRequestError = error;
            if (!result.isDone()) {
                boolean isRetriable = failoverPolicy.onError(request, error);
                if (!isRetriable) {
                    result.completeExceptionally(error);
                } else if (!stopped && requestsSent < attemptCount) {
                    send(handler);
                } else if (requestsError == requestsSent) {
                    result.completeExceptionally(error);
                }
            }
        }

        public void sendImpl(RpcClient client, FailoverResponseHandler handler) {
            long now = System.currentTimeMillis();
            if (now >= globalDeadline) {
                onGlobalTimeout();
                return;
            }

            if (requestsSent > 0) {
                metricsHolder.failoverInc();
            }

            requestsSent++;

            metricsHolder.inflightInc();
            metricsHolder.totalInc();

            request.header().setTimeout((globalDeadline - now) * 1000); // in microseconds
            cancellation.add(client.send(request, handler));

            // schedule next step
            serializedExecutorService.schedule(
                    () -> {
                        if (!result.isDone()) {
                            boolean isTimeoutRetriable = !stopped && requestsSent < attemptCount && failoverPolicy.onTimeout();
                            if (isTimeoutRetriable) {
                                send(handler);
                            }
                        }
                    }, failoverTimeout, TimeUnit.MILLISECONDS);
        }

        public void cancel() {
            for (RpcClientRequestControl control : cancellation) {
                metricsHolder.inflightDec();
                control.cancel();
            }
        }
    }
}
