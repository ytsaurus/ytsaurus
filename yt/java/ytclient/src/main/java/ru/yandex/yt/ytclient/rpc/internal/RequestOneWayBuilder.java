package ru.yandex.yt.ytclient.rpc.internal;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import com.google.protobuf.MessageLite;

import ru.yandex.yt.ytclient.rpc.RpcClient;
import ru.yandex.yt.ytclient.rpc.RpcClientResponseHandler;
import ru.yandex.yt.rpc.TRequestHeader;

public class RequestOneWayBuilder<RequestType extends MessageLite.Builder> extends RequestBuilderBase<RequestType, Void> {
    public RequestOneWayBuilder(RpcClient client, TRequestHeader.Builder header, RequestType body) {
        super(client, header, body);
    }

    @Override
    protected RpcClientResponseHandler createHandler(CompletableFuture<Void> result) {
        return new RpcClientResponseHandler() {
            @Override
            public void onAcknowledgement() {
                if (!result.isDone()) {
                    result.complete(null);
                }
            }

            @Override
            public void onResponse(List<byte[]> attachments) {
                if (!result.isDone()) {
                    result.completeExceptionally(new IllegalStateException("Server replied to a one-way request"));
                }
            }

            @Override
            public void onError(Throwable error) {
                if (!result.isDone()) {
                    result.completeExceptionally(error);
                }
            }
        };
    }
}
