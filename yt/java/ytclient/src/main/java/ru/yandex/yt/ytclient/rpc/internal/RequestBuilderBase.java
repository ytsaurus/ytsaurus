package ru.yandex.yt.ytclient.rpc.internal;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import com.google.protobuf.MessageLite;

import ru.yandex.yt.rpc.TRequestHeader;
import ru.yandex.yt.ytclient.rpc.RpcClient;
import ru.yandex.yt.ytclient.rpc.RpcClientRequestBuilder;
import ru.yandex.yt.ytclient.rpc.RpcClientRequestControl;
import ru.yandex.yt.ytclient.rpc.RpcClientResponseHandler;
import ru.yandex.yt.ytclient.rpc.RpcUtil;

public abstract class RequestBuilderBase<RequestType extends MessageLite.Builder, ResponseType> implements RpcClientRequestBuilder<RequestType, ResponseType> {
    private final RpcClient client;
    private final TRequestHeader.Builder header;
    private final RequestType body;
    private final List<byte[]> attachments = new ArrayList<>();
    private boolean requestAck = true;

    protected RequestBuilderBase(RpcClient client, TRequestHeader.Builder header, RequestType body) {
        this.client = client;
        this.header = header;
        this.body = body;
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
    public CompletableFuture<ResponseType> invoke() {
        CompletableFuture<ResponseType> result = new CompletableFuture<>();
        RpcClientResponseHandler handler = createHandler(result);
        RpcClientRequestControl control = client.send(this, handler);
        result.whenComplete((ignoredResult, ignoredException) -> control.cancel());
        return result;
    }

    protected abstract RpcClientResponseHandler createHandler(CompletableFuture<ResponseType> result);

    @Override
    public String toString() {
        return String.format("%s/%s/%s", getService(), getMethod(), getRequestId());
    }
}
