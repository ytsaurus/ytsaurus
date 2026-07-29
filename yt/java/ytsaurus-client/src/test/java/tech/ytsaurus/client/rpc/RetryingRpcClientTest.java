package tech.ytsaurus.client.rpc;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import com.google.protobuf.ByteString;
import org.junit.After;
import org.junit.Test;
import tech.ytsaurus.TError;
import tech.ytsaurus.client.RetryPolicy;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.core.common.YTsaurusError;
import tech.ytsaurus.rpc.TRequestHeader;
import tech.ytsaurus.rpc.TResponseHeader;
import tech.ytsaurus.rpcproxy.TReqGetNode;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static tech.ytsaurus.testlib.FutureUtils.getError;
import static tech.ytsaurus.testlib.FutureUtils.waitFuture;

public class RetryingRpcClientTest {
    private static final int REQUEST_QUEUE_SIZE_LIMIT_EXCEEDED = 904;
    private static final int GENERIC_ERROR_CODE = 1;
    private static final long WAIT_TIMEOUT_MS = 30_000;

    private final ScheduledThreadPoolExecutor executorService = new ScheduledThreadPoolExecutor(4);

    @After
    public void shutdown() {
        executorService.shutdownNow();
    }

    @Test
    public void retriesRetriableErrorUntilAttemptLimit() {
        RecordingRpcClient rpcClient = new RecordingRpcClient(
                (attempt, handler) -> handler.onError(error(REQUEST_QUEUE_SIZE_LIMIT_EXCEEDED)));

        CompletableFuture<String> result = send(rpcClient, retryingOptions(3));

        waitFuture(result, WAIT_TIMEOUT_MS);
        assertTrue(result.isCompletedExceptionally());
        assertTrue(hasErrorCode(getError(result), REQUEST_QUEUE_SIZE_LIMIT_EXCEEDED));
        assertEquals(3, rpcClient.sendCount.get());
    }

    @Test
    public void deliversResponseAfterSuccessfulRetry() {
        RecordingRpcClient rpcClient = new RecordingRpcClient((attempt, handler) -> { });
        rpcClient.behavior = (attempt, handler) -> {
            if (attempt == 1) {
                handler.onError(error(REQUEST_QUEUE_SIZE_LIMIT_EXCEEDED));
            } else {
                handler.onResponse(rpcClient, TResponseHeader.getDefaultInstance(), List.of(new byte[0]));
            }
        };

        CompletableFuture<String> result = send(rpcClient, retryingOptions(3));

        waitFuture(result, WAIT_TIMEOUT_MS);
        assertFalse(result.isCompletedExceptionally());
        assertEquals(2, rpcClient.sendCount.get());
    }

    @Test
    public void marksRetriesWithFreshRequestIdAndRetryFlag() {
        RecordingRpcClient rpcClient = new RecordingRpcClient(
                (attempt, handler) -> handler.onError(error(REQUEST_QUEUE_SIZE_LIMIT_EXCEEDED)));

        CompletableFuture<String> result = send(rpcClient, retryingOptions(3));

        waitFuture(result, WAIT_TIMEOUT_MS);
        assertEquals(3, rpcClient.headers.size());
        assertFalse(rpcClient.headers.get(0).getRetry());
        assertTrue(rpcClient.headers.get(1).getRetry());
        assertTrue(rpcClient.headers.get(2).getRetry());
        assertEquals("every attempt must have its own request id", 3,
                rpcClient.headers.stream().map(TRequestHeader::getRequestId).distinct().count());
    }

    @Test
    public void doesNotRetryNonRetriableError() {
        RecordingRpcClient rpcClient = new RecordingRpcClient(
                (attempt, handler) -> handler.onError(error(GENERIC_ERROR_CODE)));

        CompletableFuture<String> result = send(rpcClient, retryingOptions(3));

        waitFuture(result, WAIT_TIMEOUT_MS);
        assertTrue(result.isCompletedExceptionally());
        assertTrue(hasErrorCode(getError(result), GENERIC_ERROR_CODE));
        assertEquals(1, rpcClient.sendCount.get());
    }

    @Test
    public void doesNotRetryWhenPolicyForbidsRetries() {
        RecordingRpcClient rpcClient = new RecordingRpcClient(
                (attempt, handler) -> handler.onError(error(REQUEST_QUEUE_SIZE_LIMIT_EXCEEDED)));
        RpcOptions options = retryingOptions(3).setRetryPolicyFactory(RetryPolicy::noRetries);

        CompletableFuture<String> result = send(rpcClient, options);

        waitFuture(result, WAIT_TIMEOUT_MS);
        assertTrue(result.isCompletedExceptionally());
        assertEquals(1, rpcClient.sendCount.get());
    }

    /** The policy alone allows 100 attempts by 100 ms, which is 10 seconds, and the deadline is 600 ms. */
    @Test
    public void keepsGlobalTimeoutAsUpperBoundOfAllAttempts() {
        RecordingRpcClient rpcClient = new RecordingRpcClient(
                (attempt, handler) -> handler.onError(error(REQUEST_QUEUE_SIZE_LIMIT_EXCEEDED)));
        RpcOptions options = new RpcOptions()
                .setGlobalTimeout(Duration.ofMillis(600))
                .setRetryPolicyFactory(() -> RetryPolicy.attemptLimited(
                        100, RetryPolicy.forCodes(REQUEST_QUEUE_SIZE_LIMIT_EXCEEDED)))
                .setMinBackoffTime(Duration.ofMillis(100))
                .setMaxBackoffTime(Duration.ofMillis(100));

        long start = System.nanoTime();
        CompletableFuture<String> result = send(rpcClient, options);
        waitFuture(result, WAIT_TIMEOUT_MS);
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;

        assertTrue(result.isCompletedExceptionally());
        assertTrue(getError(result).getCause() instanceof TimeoutException);
        assertTrue("expected more than one attempt, got " + rpcClient.sendCount.get(),
                rpcClient.sendCount.get() > 1);
        assertTrue("request took " + elapsedMs + " ms, expected it to stop at the global timeout",
                elapsedMs < 3000);
    }

    /** The default policy retries timeouts, so this request would be hedged if failover were on. */
    @Test
    public void doesNotHedgeRequestThatTimesOut() {
        RecordingRpcClient rpcClient = new RecordingRpcClient((attempt, handler) -> { });
        RpcOptions options = new RpcOptions().setGlobalTimeout(Duration.ofMillis(400));

        CompletableFuture<String> result = send(rpcClient, options);

        waitFuture(result, WAIT_TIMEOUT_MS);
        assertTrue(getError(result).getCause() instanceof TimeoutException);
        assertEquals(1, rpcClient.sendCount.get());
    }

    @Test
    public void doesNotHedgeRequestWithItsOwnLongerTimeout() {
        RecordingRpcClient rpcClient = new RecordingRpcClient((attempt, handler) -> { });
        RpcOptions options = new RpcOptions()
                .setGlobalTimeout(Duration.ofMillis(200))
                .setFailoverTimeout(Duration.ofMillis(200));

        CompletableFuture<String> result = new CompletableFuture<>();
        RetryingRpcClient retryingClient = new RetryingRpcClient(rpcClient);
        retryingClient.send(retryingClient, requestWithTimeout(Duration.ofMillis(700)), handler(result), options);

        waitFuture(result, WAIT_TIMEOUT_MS);
        assertTrue(result.isCompletedExceptionally());
        assertEquals(1, rpcClient.sendCount.get());
    }

    @Test
    public void stopsRetryingWhenRequestIsCancelled() throws InterruptedException {
        Duration backoff = Duration.ofMillis(100);
        CountDownLatch firstAttempt = new CountDownLatch(1);
        CountDownLatch twoAttemptsObserved = new CountDownLatch(2);
        RecordingRpcClient rpcClient = new RecordingRpcClient((attempt, handler) -> {
            firstAttempt.countDown();
            twoAttemptsObserved.countDown();
            handler.onError(error(REQUEST_QUEUE_SIZE_LIMIT_EXCEEDED));
        });
        RpcOptions options = new RpcOptions()
                .setRetryPolicyFactory(() -> RetryPolicy.attemptLimited(
                        100, RetryPolicy.forCodes(REQUEST_QUEUE_SIZE_LIMIT_EXCEEDED)))
                .setMinBackoffTime(backoff)
                .setMaxBackoffTime(backoff);

        CompletableFuture<String> result = new CompletableFuture<>();
        RetryingRpcClient retryingClient = new RetryingRpcClient(rpcClient);
        RpcClientRequestControl control =
                retryingClient.send(retryingClient, requestWithTimeout(Duration.ofSeconds(30)),
                        handler(result), options);

        assertTrue("the first attempt has not been sent",
                firstAttempt.await(WAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS));
        control.cancel();

        // the next attempt is already scheduled by now, wait for several backoffs to see that it is not sent
        assertFalse("a retry has been sent after the request was cancelled",
                twoAttemptsObserved.await(5 * backoff.toMillis(), TimeUnit.MILLISECONDS));
        assertEquals(1, rpcClient.sendCount.get());
        assertTrue("the cancelled request must be reported to the handler", result.isCompletedExceptionally());
    }

    private CompletableFuture<String> send(RpcClient rpcClient, RpcOptions options) {
        CompletableFuture<String> result = new CompletableFuture<>();
        RetryingRpcClient retryingClient = new RetryingRpcClient(rpcClient);
        retryingClient.send(retryingClient, requestWithTimeout(options.getGlobalTimeout()), handler(result), options);
        return result;
    }

    private static RpcClientResponseHandler handler(CompletableFuture<String> result) {
        return new RpcClientResponseHandler() {
            @Override
            public void onResponse(RpcClient sender, TResponseHeader header, List<byte[]> attachments) {
                result.complete("response");
            }

            @Override
            public void onError(Throwable error) {
                result.completeExceptionally(error);
            }

            @Override
            public void onCancel(CancellationException cancel) {
                result.completeExceptionally(cancel);
            }
        };
    }

    private static RpcRequest<?> requestWithTimeout(Duration timeout) {
        TReqGetNode body = TReqGetNode.newBuilder().setPath(ByteString.copyFromUtf8("/")).build();
        TRequestHeader.Builder header = TRequestHeader.newBuilder()
                .setService("ApiService")
                .setMethod("GetNode")
                .setRequestId(RpcUtil.toProto(GUID.create()))
                .setTimeout(RpcUtil.durationToMicros(timeout));
        return new RpcRequest<>(header.build(), body, List.of());
    }

    private static RpcOptions retryingOptions(int attemptLimit) {
        return new RpcOptions()
                .setRetryPolicyFactory(() -> RetryPolicy.attemptLimited(
                        attemptLimit, RetryPolicy.forCodes(REQUEST_QUEUE_SIZE_LIMIT_EXCEEDED)))
                .setMinBackoffTime(Duration.ZERO)
                .setMaxBackoffTime(Duration.ZERO);
    }

    private static YTsaurusError error(int code) {
        return new YTsaurusError(TError.newBuilder()
                .setCode(code)
                .setMessage("error with code " + code)
                .build());
    }

    private static boolean hasErrorCode(Throwable error, int code) {
        for (Throwable current = error; current != null; current = current.getCause()) {
            if (current instanceof YTsaurusError && ((YTsaurusError) current).matches(code)) {
                return true;
            }
            if (current.getCause() == current) {
                break;
            }
        }
        return false;
    }

    private final class RecordingRpcClient extends RpcClientTestStubs.RpcClientStub {
        private final AtomicInteger sendCount = new AtomicInteger();
        private final List<TRequestHeader> headers = new CopyOnWriteArrayList<>();
        private BiConsumer<Integer, RpcClientResponseHandler> behavior;

        RecordingRpcClient(BiConsumer<Integer, RpcClientResponseHandler> behavior) {
            super("recording-client");
            this.behavior = behavior;
        }

        @Override
        public ScheduledExecutorService executor() {
            return executorService;
        }

        @Override
        public RpcClientRequestControl send(
                RpcClient sender,
                RpcRequest<?> request,
                RpcClientResponseHandler handler,
                RpcOptions options
        ) {
            headers.add(request.header);
            behavior.accept(sendCount.incrementAndGet(), handler);
            return () -> false;
        }
    }
}
