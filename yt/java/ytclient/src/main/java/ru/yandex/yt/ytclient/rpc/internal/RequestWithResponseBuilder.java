package ru.yandex.yt.ytclient.rpc.internal;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;

import com.google.protobuf.MessageLite;
import com.google.protobuf.Parser;

import ru.yandex.lang.NonNullApi;
import ru.yandex.yt.rpc.TRequestHeader;
import ru.yandex.yt.rpc.TResponseHeader;
import ru.yandex.yt.ytclient.rpc.RpcClient;
import ru.yandex.yt.ytclient.rpc.RpcClientResponse;
import ru.yandex.yt.ytclient.rpc.RpcClientResponseHandler;
import ru.yandex.yt.ytclient.rpc.RpcOptions;

@NonNullApi
public class RequestWithResponseBuilder<RequestType extends MessageLite.Builder, ResponseType extends MessageLite> extends RequestBuilderBase<RequestType, RpcClientResponse<ResponseType>> {
    private final Parser<ResponseType> parser;

    public RequestWithResponseBuilder(
            TRequestHeader.Builder header, RequestType body,
            Parser<ResponseType> parser,
            RpcOptions options) {
        super(header, body, options);
        this.parser = parser;
    }

    @Override
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
}
