package ru.yandex.yt.ytclient.proxy.internal;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.junit.After;
import org.junit.Test;

import ru.yandex.inside.yt.kosher.impl.common.YtException;
import ru.yandex.yt.rpc.TRequestHeader;
import ru.yandex.yt.rpc.TResponseHeader;
import ru.yandex.yt.rpcproxy.TReqGetNode;
import ru.yandex.yt.ytclient.proxy.RetryPolicy;
import ru.yandex.yt.ytclient.rpc.RpcClient;
import ru.yandex.yt.ytclient.rpc.RpcClientPool;
import ru.yandex.yt.ytclient.rpc.RpcClientRequestControl;
import ru.yandex.yt.ytclient.rpc.RpcClientResponseHandler;
import ru.yandex.yt.ytclient.rpc.RpcClientStreamControl;
import ru.yandex.yt.ytclient.rpc.RpcFailoverPolicy;
import ru.yandex.yt.ytclient.rpc.RpcOptions;
import ru.yandex.yt.ytclient.rpc.RpcRequest;
import ru.yandex.yt.ytclient.rpc.RpcStreamConsumer;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.fail;
import static ru.yandex.yt.testlib.FutureUtils.getError;
import static ru.yandex.yt.testlib.Matchers.isCausedBy;

public class FailoverRpcExecutorTest {
    ScheduledExecutorService executorService = new ScheduledThreadPoolExecutor(4);

    @After
    public void after() {
        executorService.shutdownNow();
    }

    @Test
    public void testCancel() {
        CompletableFuture<String> result = new CompletableFuture<>();

        Supplier<RetryPolicy> retryPolicyFactory = () -> RetryPolicy.attemptLimited(
                2, RetryPolicy.fromRpcFailoverPolicy(new TestFailoverPolicy()));

        RpcClientRequestControl c = execute(
                responseNever(),
                defaultOptions().setRetryPolicyFactory(retryPolicyFactory),
                result,
                2);
        c.cancel();
        waitFuture(result, 10);

        assertThat(result.isDone(), is(true));
        assertThat(result.isCancelled(), is(true));
    }

    @Test
    public void testSuccess() throws Exception {
        CompletableFuture<String> result = new CompletableFuture<>();
        Supplier<RetryPolicy> retryPolicyFactory = () -> RetryPolicy.attemptLimited(
                2, RetryPolicy.fromRpcFailoverPolicy(new TestFailoverPolicy()));

        execute(responseImmediately(), defaultOptions().setRetryPolicyFactory(retryPolicyFactory), result, 2);

        waitFuture(result, 1000);

        assertThat(result.isDone(), is(true));
        assertThat(result.isCompletedExceptionally(), is(false));
        assertThat(result.get(), is("response"));
    }

    @Test
    public void testGlobalTimeout() {
        CompletableFuture<String> result = new CompletableFuture<>();
        Supplier<RetryPolicy> retryPolicyFactory = () -> RetryPolicy.attemptLimited(
                2, RetryPolicy.fromRpcFailoverPolicy(new TestFailoverPolicy()));

        RpcOptions options = defaultOptions()
                .setGlobalTimeout(Duration.ofMillis(100))
                .setFailoverTimeout(Duration.ofMillis(100))
                .setRetryPolicyFactory(retryPolicyFactory);

        execute(responseNever(), options, result, 2);

        waitFuture(result, 1000);

        assertThat(result.isDone(), is(true));
        assertThat(result.isCompletedExceptionally(), is(true));
        assertThat(getError(result), isCausedBy(TimeoutException.class));
    }

    @Test
    public void testFailover() throws Exception {
        CompletableFuture<String> result = new CompletableFuture<>();
        Supplier<RetryPolicy> retryPolicyFactory = () -> RetryPolicy.attemptLimited(
                2, RetryPolicy.fromRpcFailoverPolicy(new TestFailoverPolicy()));
        RpcOptions options = defaultOptions()
                .setGlobalTimeout(Duration.ofMillis(1000))
                .setFailoverTimeout(Duration.ofMillis(100))
                .setRetryPolicyFactory(retryPolicyFactory);
        execute(responseOnSecondRequest(), options, result, 2);

        waitFuture(result, 1000);

        assertThat(result.isDone(), is(true));
        assertThat(result.isCompletedExceptionally(), is(false));
        assertThat(result.get(), is("response"));
    }

    @Test
    public void testNoClientsInPool() {
        CompletableFuture<String> result = new CompletableFuture<>();

        Supplier<RetryPolicy> retryPolicyFactory = () -> RetryPolicy.attemptLimited(
                2, RetryPolicy.fromRpcFailoverPolicy(new TestFailoverPolicy()));

        execute(
                responseNever(),
                defaultOptions().setRetryPolicyFactory(retryPolicyFactory),
                result,
                0);

        waitFuture(result, 1000);

        assertThat(result.isDone(), is(true));
        assertThat(result.isCompletedExceptionally(), is(true));
        assertThat(getError(result).toString(), containsString("pool is exhausted"));
    }

    @Test
    public void respondWithDelayPoolExhausted() throws ExecutionException, InterruptedException {
        CompletableFuture<String> result = new CompletableFuture<>();

        Supplier<RetryPolicy> retryPolicyFactory = () -> RetryPolicy.attemptLimited(
                2, RetryPolicy.fromRpcFailoverPolicy(new TestFailoverPolicy()));

        Consumer<RpcClientResponseHandler> respondWithDelay = (handler) -> executorService.schedule(
                () -> handler.onResponse(null, null, null),
                100, TimeUnit.MILLISECONDS);

        execute(respondWithDelay, defaultOptions().setRetryPolicyFactory(retryPolicyFactory), result, 1);

        waitFuture(result, 1000);

        assertThat(result.isDone(), is(true));
        assertThat(result.isCompletedExceptionally(), is(false));
        assertThat(result.get(), is("response"));
    }

    @Test
    public void errorImmediatelyPoolExhausted() {
        CompletableFuture<String> result = new CompletableFuture<>();

        Supplier<RetryPolicy> retryPolicyFactory = () -> RetryPolicy.attemptLimited(
                2, RetryPolicy.fromRpcFailoverPolicy(new TestFailoverPolicy()));

        AtomicInteger attempts = new AtomicInteger(0);
        Consumer<RpcClientResponseHandler> respondWithError = (handler) -> {
            attempts.incrementAndGet();
            handler.onError(new YtException("retriable error"));
        };

        execute(respondWithError, defaultOptions().setRetryPolicyFactory(retryPolicyFactory), result, 2);

        waitFuture(result, 1000);

        assertThat(result.isDone(), is(true));
        assertThat(result.isCompletedExceptionally(), is(true));
        assertThat(getError(result).toString(), containsString("retriable error"));
        assertThat(attempts.get(), is(2));
    }

    @Test
    public void errorWithDelayPoolExhausted() {
        CompletableFuture<String> result = new CompletableFuture<>();

        Supplier<RetryPolicy> retryPolicyFactory = () -> RetryPolicy.attemptLimited(
                2, RetryPolicy.fromRpcFailoverPolicy(new TestFailoverPolicy()));

        RpcOptions options = defaultOptions()
                .setGlobalTimeout(Duration.ofMillis(1000))
                .setFailoverTimeout(Duration.ofMillis(20))
                .setRetryPolicyFactory(retryPolicyFactory);

        Consumer<RpcClientResponseHandler> respondWithDelay = (handler) -> executorService.schedule(
                () -> handler.onError(new YtException("our test error")),
                100, TimeUnit.MILLISECONDS);

        execute(respondWithDelay, options, result, 1);

        waitFuture(result, 1000);

        assertThat(result.isDone(), is(true));
        assertThat(result.isCompletedExceptionally(), is(true));
        assertThat(getError(result).toString(), containsString("our test error"));
    }

    private RpcClient createClient(Consumer<RpcClientResponseHandler> handlerConsumer) {
        return new RpcClient() {
            @Override
            public void ref() {
            }

            @Override
            public void unref() {
            }

            @Override
            public void close() {
            }

            @Override
            public RpcClientRequestControl send(
                    RpcClient sender,
                    RpcRequest<?> request,
                    RpcClientResponseHandler handler,
                    RpcOptions options)
            {
                handlerConsumer.accept(handler);
                return () -> false;
            }

            @Override
            public RpcClientStreamControl startStream(
                    RpcClient sender,
                    RpcRequest<?> request,
                    RpcStreamConsumer consumer,
                    RpcOptions options)
            {
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
        };
    }

    private Consumer<RpcClientResponseHandler> responseNever() {
        return handler -> {
        };
    }

    private Consumer<RpcClientResponseHandler> responseImmediately() {
        return handler -> handler.onResponse(null, null, null);
    }

    private Consumer<RpcClientResponseHandler> responseOnSecondRequest() {
        return new Consumer<>() {
            int count = 1;

            @Override
            public void accept(RpcClientResponseHandler handler) {
                if (count >= 2) {
                    handler.onResponse(null, null, null);
                } else {
                    count += 1;
                }
            }
        };
    }

    private RpcClientRequestControl execute(
            Consumer<RpcClientResponseHandler> handlerConsumer,
            RpcOptions options,
            CompletableFuture<String> result,
            int clientCount)
    {
        RpcRequest<?> rpcRequest;
        {
            TReqGetNode reqGetNode = TReqGetNode.newBuilder().setPath("/").build();
            TRequestHeader.Builder header = TRequestHeader.newBuilder();
            header.setService("service");
            header.setMethod("method");
            rpcRequest = new RpcRequest<>(header.build(), reqGetNode, List.of());
        }

        RpcClientResponseHandler handler = new RpcClientResponseHandler() {
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

        List<RpcClient> clients = new ArrayList<>();
        RpcClient client = createClient(handlerConsumer);
        for (int i = 0; i < clientCount; ++i) {
            clients.add(client);
        }

        return FailoverRpcExecutor.execute(
                executorService,
                RpcClientPool.collectionPool(clients),
                rpcRequest,
                handler,
                options);
    }

    private static void waitFuture(Future<?> future, long timeoutMillis) {
        try {
            future.get(timeoutMillis, TimeUnit.MILLISECONDS);
        } catch (InterruptedException | TimeoutException error) {
            fail(error.toString());
        } catch (ExecutionException | CancellationException error) {
            // that's ok
        }
    }

    static RpcOptions defaultOptions() {
        RpcOptions result = new RpcOptions();
        result.setFailoverPolicy(new TestFailoverPolicy());
        return result;
    }

    private static class TestFailoverPolicy implements RpcFailoverPolicy {
        @Override
        public boolean onError(Throwable error) {
            return error.getMessage().contains("retriable");
        }

        @Override
        public boolean onTimeout() {
            return true;
        }

        @Override
        public boolean randomizeDcs() {
            return false;
        }
    }
}
