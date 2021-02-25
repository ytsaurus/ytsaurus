package ru.yandex.yt.ytclient.rpc.internal;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

import com.google.protobuf.MessageLite;
import com.google.protobuf.Parser;

import ru.yandex.lang.NonNullApi;
import ru.yandex.yt.rpc.TRequestHeader;
import ru.yandex.yt.rpc.TResponseHeader;
import ru.yandex.yt.rpc.TStreamingFeedbackHeader;
import ru.yandex.yt.rpc.TStreamingPayloadHeader;
import ru.yandex.yt.ytclient.proxy.internal.FailoverRpcExecutor;
import ru.yandex.yt.ytclient.rpc.RpcClient;
import ru.yandex.yt.ytclient.rpc.RpcClientPool;
import ru.yandex.yt.ytclient.rpc.RpcClientRequestBuilder;
import ru.yandex.yt.ytclient.rpc.RpcClientRequestControl;
import ru.yandex.yt.ytclient.rpc.RpcClientResponse;
import ru.yandex.yt.ytclient.rpc.RpcClientResponseHandler;
import ru.yandex.yt.ytclient.rpc.RpcClientStreamControl;
import ru.yandex.yt.ytclient.rpc.RpcOptions;
import ru.yandex.yt.ytclient.rpc.RpcRequest;
import ru.yandex.yt.ytclient.rpc.RpcStreamConsumer;
import ru.yandex.yt.ytclient.rpc.RpcUtil;

@NonNullApi
public class RequestWithResponseBuilder<RequestType extends MessageLite.Builder, ResponseType extends MessageLite>
        implements RpcClientRequestBuilder<RequestType, RpcClientResponse<ResponseType>>
{
    private final TRequestHeader.Builder header;
    private final RequestType body;
    private final List<byte[]> attachments = new ArrayList<>();
    private final RpcOptions options;
    private final Parser<ResponseType> parser;

    public RequestWithResponseBuilder(
            TRequestHeader.Builder header, RequestType body,
            Parser<ResponseType> parser,
            RpcOptions options) {
        this.header = header;
        this.body = body;
        this.options = options;
        if (options.getGlobalTimeout() == null) {
            header().clearTimeout();
        } else {
            header().setTimeout(RpcUtil.durationToMicros(options.getGlobalTimeout()));
        }
        this.parser = parser;
    }

    @Override
    public RpcOptions getOptions() {
        return this.options;
    }

    @Override
    public TRequestHeader.Builder header() {
        return header;
    }

    @Override
    public RequestType body() {
        return body;
    }

    @Override
    public List<byte[]> attachments() {
        return attachments;
    }

    @Override
    public RpcRequest<?> getRpcRequest() {
        return new RpcRequest<>(header.build(), body.build(), attachments);
    }

    @Override
    public CompletableFuture<RpcClientResponse<ResponseType>> invoke(RpcClient client) {
        CompletableFuture<RpcClientResponse<ResponseType>> result = new CompletableFuture<>();
        try {
            RpcClientResponseHandler handler = createHandler(result);
            RpcClientRequestControl control = client.send(client, getRpcRequest(), handler, getOptions());
            result.whenComplete((ignoredResult, ignoredException) -> control.cancel());
        } catch (Throwable e) {
            result.completeExceptionally(e);
        }
        return result;
    }

    @Override
    public CompletableFuture<RpcClientResponse<ResponseType>> invokeVia(ScheduledExecutorService executor, RpcClientPool clientPool) {
        CompletableFuture<RpcClientResponse<ResponseType>> result = new CompletableFuture<>();
        try {
            RpcClientResponseHandler handler = createHandler(result);
            // NB. we use attemptCount 3 since it corresponds to old default behaviour
            final int attemptCount = 3;
            RpcClientRequestControl control = FailoverRpcExecutor.execute(
                    executor,
                    clientPool,
                    getRpcRequest(),
                    handler,
                    getOptions(),
                    attemptCount);
            result.whenComplete((ignoredResult, ignoredException) -> control.cancel());
        } catch (Throwable e) {
            result.completeExceptionally(e);
        }
        return result;
    }

    @Override
    public RpcClientStreamControl startStream(RpcClient client, RpcStreamConsumer consumer) {
        return client.startStream(client, getRpcRequest(), consumer, getOptions());
    }

    @Override
    public CompletableFuture<RpcClientStreamControl> startStream(
            ScheduledExecutorService executor,
            RpcClientPool clientPool,
            RpcStreamConsumer consumer)
    {
        CompletableFuture<Void> clientReleaseFuture = new CompletableFuture<>();
        RpcStreamConsumer wrappedConsumer = new RpcStreamConsumer() {
            @Override
            public void onStartStream(RpcClientStreamControl control) {
                consumer.onStartStream(control);
            }

            @Override
            public void onFeedback(RpcClient sender, TStreamingFeedbackHeader header, List<byte[]> attachments) {
                consumer.onFeedback(sender, header, attachments);
            }

            @Override
            public void onPayload(RpcClient sender, TStreamingPayloadHeader header, List<byte[]> attachments) {
                consumer.onPayload(sender, header, attachments);
            }

            @Override
            public void onResponse(RpcClient sender, TResponseHeader header, List<byte[]> attachments) {
                consumer.onResponse(sender, header, attachments);
                clientReleaseFuture.complete(null);
            }

            @Override
            public void onError(Throwable cause) {
                consumer.onError(cause);
                clientReleaseFuture.complete(null);
            }

            @Override
            public void onCancel(CancellationException cancel) {
                consumer.onCancel(cancel);
                clientReleaseFuture.complete(null);
            }

            @Override
            public void onWakeup() {
                consumer.onWakeup();
            }
        };
        CompletableFuture<RpcClient> clientFuture = clientPool.peekClient(clientReleaseFuture);
        return clientFuture.thenApply(client -> client.startStream(client, getRpcRequest(), wrappedConsumer, getOptions()));
    }

    public RpcClientResponseHandler createHandler(CompletableFuture<RpcClientResponse<ResponseType>> result) {
        return new RpcClientResponseHandler() {
            @Override
            public void onResponse(RpcClient sender, TResponseHeader header, List<byte[]> attachments) {
                if (!result.isDone()) {
                    if (attachments.size() < 1 || attachments.get(0) == null) {
                        throw new IllegalStateException("Received response without a body");
                    }
                    result.complete(
                            new LazyResponse<>(
                                    parser,
                                    attachments.get(0),
                                    new ArrayList<>(attachments.subList(1, attachments.size())),
                                    sender,
                                    header));
                }
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

    @Override
    public String toString() {
        return String.format("%s/%s/%s", getService(), getMethod(), getRequestId());
    }
}
