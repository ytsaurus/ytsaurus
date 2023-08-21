package tech.ytsaurus.client.rpc;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ytsaurus.client.RetryPolicy;
import tech.ytsaurus.client.misc.ScheduledSerializedExecutorService;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.rpc.TRequestHeader;
import tech.ytsaurus.rpc.TResponseHeader;

class FailoverRpcExecutor {
    private static final Logger logger = LoggerFactory.getLogger(FailoverRpcExecutor.class);
    private static final TimeoutException TIMEOUT_EXCEPTION = new TimeoutException();

    private final ScheduledSerializedExecutorService serializedExecutorService;
    private final BalancingResponseHandlerMetricsHolder metricsHolder;
    private final RpcClientPool clientPool;
    private final RetryPolicy retryPolicy;

    private final long failoverTimeout;
    private final long globalDeadline;

    private final RpcRequest<?> request;
    private final GUID originalRequestId;
    private final RpcClientResponseHandler baseHandler;
    private final RpcOptions options;

    private final CompletableFuture<Result> result = new CompletableFuture<>();

    private final MutableState mutableState;

    private FailoverRpcExecutor(
            ScheduledExecutorService executorService,
            RpcClientPool clientPool,
            RpcRequest<?> request,
            RpcClientResponseHandler handler,
            RpcOptions options
    ) {
        this.serializedExecutorService = new ScheduledSerializedExecutorService(executorService);
        this.clientPool = clientPool;
        this.metricsHolder = options.getResponseMetricsHolder();

        this.retryPolicy = options.getRetryPolicyFactory().get();

        this.failoverTimeout = options.getFailoverTimeout().toMillis();
        this.globalDeadline = System.currentTimeMillis() + options.getGlobalTimeout().toMillis();

        this.request = request;
        this.originalRequestId = RpcRequest.getRequestId(request.header);
        this.baseHandler = handler;
        this.options = options;

        this.mutableState = new MutableState();
    }

    private RpcClientRequestControl execute() {
        serializedExecutorService.execute(() -> mutableState.executeImpl(new FailoverResponseHandler()));

        result.whenComplete((result, error) -> {
            if (error != null) {
                logger.warn("Request {} failed with error; OriginalRequestId: {}, Error: {}",
                        request, originalRequestId, error.toString());
            }
            serializedExecutorService.submit(mutableState::cancel);
            handleResult(result, error);
        });

        return () -> result.cancel(true);
    }

    public static RpcClientRequestControl execute(
            ScheduledExecutorService executorService,
            RpcClientPool clientPool,
            RpcRequest<?> request,
            RpcClientResponseHandler handler,
            RpcOptions options
    ) {
        return new FailoverRpcExecutor(
                executorService,
                clientPool,
                request,
                handler,
                options)
                .execute();
    }

    private void send(RpcClientResponseHandler handler) {
        logger.trace("Peeking connection from pool; OriginalRequestId: {}", originalRequestId);
        clientPool.peekClient(result).whenCompleteAsync((RpcClient client, Throwable error) -> {
            if (error == null) {
                mutableState.sendImpl(client, handler);
                return;
            }

            logger.warn("Failed to get RpcClient from pool; OriginalRequestId: {}", originalRequestId, error);
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
        result.completeExceptionally(
                new TimeoutException(
                        String.format("Request has timed out; OriginalRequestId: %s", originalRequestId))
        );
    }

    private static class Result {
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
        // Otherwise, no other retry will be performed, if we get
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
                Optional<Duration> backoffDuration = retryPolicy.getBackoffDuration(error, options);
                boolean isRetriable = backoffDuration.isPresent();
                if (!isRetriable) {
                    result.completeExceptionally(error);
                } else if (!stopped) {
                    serializedExecutorService.schedule(
                            () -> send(handler),
                            backoffDuration.get().toMillis(),
                            TimeUnit.MILLISECONDS
                    );
                } else if (requestsError == requestsSent) {
                    result.completeExceptionally(error);
                }
            }
        }

        public void sendImpl(RpcClient client, RpcClientResponseHandler handler) {
            long now = System.currentTimeMillis();
            if (now >= globalDeadline) {
                onGlobalTimeout();
                return;
            }

            if (requestsSent > 0) {
                metricsHolder.failoverInc();
            }

            requestsSent++;
            retryPolicy.onNewAttempt();

            metricsHolder.inflightInc();
            metricsHolder.totalInc();

            TRequestHeader.Builder requestHeader = request.header.toBuilder();
            requestHeader.setTimeout((globalDeadline - now) * 1000);  // in microseconds

            GUID currentRequestId;

            if (requestsSent > 1) {
                currentRequestId = GUID.create();
                requestHeader.setRequestId(RpcUtil.toProto(currentRequestId));
                requestHeader.setRetry(true);
            } else {
                currentRequestId = originalRequestId;
                requestHeader.setRequestId(RpcUtil.toProto(currentRequestId));
            }

            RpcRequest<?> copy = request.copy(requestHeader.build());
            cancellation.add(client.send(client, copy, handler, options));

            logger.debug("Starting new attempt; AttemptId: {}, OriginalRequestId: {}, RequestId: {}",
                    requestsSent, originalRequestId, currentRequestId);

            // schedule next step
            ScheduledFuture<?> scheduled = serializedExecutorService.schedule(
                    () -> {
                        if (!result.isDone()) {
                            Optional<Duration> backoffDuration =
                                    retryPolicy.getBackoffDuration(TIMEOUT_EXCEPTION, options);
                            boolean isTimeoutRetriable = !stopped && backoffDuration.isPresent();
                            if (isTimeoutRetriable) {
                                serializedExecutorService.schedule(
                                        () -> send(handler),
                                        backoffDuration.get().toMillis(),
                                        TimeUnit.MILLISECONDS
                                );
                            }
                        }
                    }, failoverTimeout, TimeUnit.MILLISECONDS);
            cancellation.add(() -> scheduled.cancel(true));
        }

        public void executeImpl(RpcClientResponseHandler handler) {
            long globalDelay = globalDeadline - System.currentTimeMillis();
            ScheduledFuture<?> scheduled = serializedExecutorService.schedule(
                    FailoverRpcExecutor.this::onGlobalTimeout,
                    globalDelay,
                    TimeUnit.MILLISECONDS);
            cancellation.add(() -> scheduled.cancel(true));
            send(handler);
        }

        public void cancel() {
            for (RpcClientRequestControl control : cancellation) {
                metricsHolder.inflightDec();
                control.cancel();
            }
        }
    }
}
