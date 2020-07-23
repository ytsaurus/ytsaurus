package ru.yandex.yt.ytclient.proxy.internal;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

import io.netty.channel.DefaultEventLoop;
import org.junit.Test;

import ru.yandex.yt.rpc.TRequestHeader;
import ru.yandex.yt.rpc.TResponseHeader;
import ru.yandex.yt.ytclient.rpc.RpcClient;
import ru.yandex.yt.ytclient.rpc.RpcClientRequest;
import ru.yandex.yt.ytclient.rpc.RpcClientRequestControl;
import ru.yandex.yt.ytclient.rpc.RpcClientResponseHandler;
import ru.yandex.yt.ytclient.rpc.RpcClientStreamControl;
import ru.yandex.yt.ytclient.rpc.RpcOptions;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FailoverRpcExecutorTest {
    @Test
    public void testCancel() {
        CompletableFuture<String> result = new CompletableFuture<>();
        FailoverRpcExecutor executor = createExecutor(responseNever(), Optional.empty(), Optional.empty(), result);

        RpcClientRequestControl c = executor.execute();
        c.cancel();

        assertThat(result.isDone(), is(true));
        assertThat(result.isCompletedExceptionally(), is(true));
        result.whenComplete((res, error) -> assertThat(error, instanceOf(CancellationException.class)));
    }

    @Test
    public void testSuccess() throws Exception {
        CompletableFuture<String> result = new CompletableFuture<>();
        FailoverRpcExecutor executor = createExecutor(responseImmediately(), Optional.empty(), Optional.empty(), result);

        executor.execute();

        assertThat(result.isDone(), is(true));
        assertThat(result.isCompletedExceptionally(), is(false));
        assertThat(result.get(), is("response"));
    }

    @Test
    public void testGlobalTimeout() throws Exception {
        CompletableFuture<String> result = new CompletableFuture<>();
        FailoverRpcExecutor executor = createExecutor(responseNever(),
                Optional.of(Duration.ofMillis(100)), Optional.of(Duration.ofMillis(100)), result);

        executor.execute();

        Thread.sleep(150);
        assertThat(result.isDone(), is(true));
        assertThat(result.isCompletedExceptionally(), is(true));
        result.whenComplete((res, error) -> assertThat(error, instanceOf(TimeoutException.class)));
    }

    @Test
    public void testFailover() throws Exception {
        CompletableFuture<String> result = new CompletableFuture<>();
        FailoverRpcExecutor executor = createExecutor(responseOnSecondRequest(),
                Optional.of(Duration.ofMillis(1000)), Optional.of(Duration.ofMillis(100)), result);

        executor.execute();

        Thread.sleep(250);
        assertThat(result.isDone(), is(true));
        assertThat(result.isCompletedExceptionally(), is(false));
        assertThat(result.get(), is("response"));
    }

    private RpcClient createClient(Consumer<RpcClientResponseHandler> handlerConsumer) {
        RpcClientRequestControl control = mock(RpcClientRequestControl.class);
        return new RpcClient() {
            @Override
            public void close() {}

            @Override
            public RpcClientRequestControl send(RpcClient sender, RpcClientRequest request, RpcClientResponseHandler handler) {
                handlerConsumer.accept(handler);
                return control;
            }

            @Override
            public RpcClientStreamControl startStream(RpcClient sender, RpcClientRequest request) {
                return null;
            }

            @Override
            public String destinationName() {
                return null;
            }

            @Override
            public ScheduledExecutorService executor() {
                return null;
            }
        };
    }

    private Consumer<RpcClientResponseHandler> responseNever() {
        return handler -> {};
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

    private FailoverRpcExecutor createExecutor(Consumer<RpcClientResponseHandler> handlerConsumer,
                                               Optional<Duration> globalTimeout,
                                               Optional<Duration> failoverTimeout,
                                               CompletableFuture<String> result) {
        ScheduledExecutorService executorService = new DefaultEventLoop();
        List<RpcClient> clients = Arrays.asList(createClient(handlerConsumer), createClient(handlerConsumer));

        RpcOptions options = new RpcOptions();
        globalTimeout.ifPresent(options::setGlobalTimeout);
        failoverTimeout.ifPresent(options::setFailoverTimeout);
        TRequestHeader.Builder header = TRequestHeader.newBuilder();
        RpcClientRequest request = mock(RpcClientRequest.class);
        when(request.getOptions()).thenReturn(options);
        when(request.header()).thenReturn(header);

        RpcClientResponseHandler handler = new RpcClientResponseHandler() {
            @Override
            public void onAcknowledgement(RpcClient sender) {
            }

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


        return new FailoverRpcExecutor(executorService, clients, request, handler);
    }
}
