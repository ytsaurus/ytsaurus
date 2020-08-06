package ru.yandex.yt.ytclient.rpc.internal;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

import com.google.protobuf.MessageLite;

import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;
import ru.yandex.yt.rpc.TRequestHeader;
import ru.yandex.yt.ytclient.proxy.internal.FailoverRpcExecutor;
import ru.yandex.yt.ytclient.rpc.RpcClient;
import ru.yandex.yt.ytclient.rpc.RpcClientPool;
import ru.yandex.yt.ytclient.rpc.RpcClientRequestBuilder;
import ru.yandex.yt.ytclient.rpc.RpcClientRequestControl;
import ru.yandex.yt.ytclient.rpc.RpcClientResponseHandler;
import ru.yandex.yt.ytclient.rpc.RpcClientStreamControl;
import ru.yandex.yt.ytclient.rpc.RpcOptions;
import ru.yandex.yt.ytclient.rpc.RpcUtil;

@NonNullApi
@NonNullFields
public abstract class RequestBuilderBase<RequestType extends MessageLite.Builder, ResponseType> implements RpcClientRequestBuilder<RequestType, ResponseType> {
    private final TRequestHeader.Builder header;
    private final RequestType body;
    private final List<byte[]> attachments = new ArrayList<>();
    private boolean requestAck = true;
    private final RpcOptions options;

    RequestBuilderBase(TRequestHeader.Builder header, RequestType body, RpcOptions options) {
        this.header = header;
        this.body = body;
        this.options = options;
        setTimeout(options.getGlobalTimeout());
        setRequestAck(options.getDefaultRequestAck());
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
    public List<byte[]> serialize() {
        return RpcUtil.createRequestMessage(header.build(), body.build(), attachments);
    }

    @Override
    public boolean requestAck() {
        return requestAck;
    }

    @Override
    public void setRequestAck(boolean requestAck) {
        this.requestAck = requestAck;
    }

    @Override
    public CompletableFuture<ResponseType> invoke(RpcClient client) {
        CompletableFuture<ResponseType> result = new CompletableFuture<>();
        try {
            RpcClientResponseHandler handler = createHandler(result);
            RpcClientRequestControl control = client.send(this, handler);
            result.whenComplete((ignoredResult, ignoredException) -> control.cancel());
        } catch (Throwable e) {
            result.completeExceptionally(e);
        }
        return result;
    }

    @Override
    public CompletableFuture<ResponseType> invokeVia(ScheduledExecutorService executor, List<RpcClient> clients) {
        CompletableFuture<ResponseType> result = new CompletableFuture<>();
        try {
            RpcClientResponseHandler handler = createHandler(result);
            RpcClientRequestControl control = sendVia(executor, handler, clients);
            result.whenComplete((ignoredResult, ignoredException) -> control.cancel());
        } catch (Throwable e) {
            result.completeExceptionally(e);
        }
        return result;
    }

    @Override
    public RpcClientStreamControl startStream(RpcClient client) {
        return client.startStream(this);
    }

    @Override
    public RpcClientStreamControl startStream(ScheduledExecutorService executor, List<RpcClient> clients) {
        if (!clients.isEmpty()) {
            return clients.get(0).startStream(this);
        } else {
            throw new IllegalStateException("client is not set");
        }
    }

    private RpcClientRequestControl sendVia(
            ScheduledExecutorService executorService,
            RpcClientResponseHandler handler,
            List<RpcClient> clients)
    {
        return FailoverRpcExecutor.execute(
            executorService,
            RpcClientPool.collectionPool(clients),
            this,
            handler,
            clients.size());
    }

    protected abstract RpcClientResponseHandler createHandler(CompletableFuture<ResponseType> result);

    @Override
    public String toString() {
        return String.format("%s/%s/%s", getService(), getMethod(), getRequestId());
    }
}
