package ru.yandex.yt.ytclient.rpc.internal;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import com.google.protobuf.MessageLite;

import ru.yandex.yt.rpc.TRequestHeader;
import ru.yandex.yt.ytclient.rpc.RpcClient;
import ru.yandex.yt.ytclient.rpc.RpcClientResponse;
import ru.yandex.yt.ytclient.rpc.RpcClientResponseHandler;
import ru.yandex.yt.ytclient.rpc.RpcMessageParser;

public class RequestWithResponseBuilder<RequestType extends MessageLite.Builder, ResponseType extends MessageLite> extends RequestBuilderBase<RequestType, RpcClientResponse<ResponseType>> {
    private final RpcMessageParser<ResponseType> parser;

    public RequestWithResponseBuilder(RpcClient client, TRequestHeader.Builder header, RequestType body,
            RpcMessageParser<ResponseType> parser)
    {
        super(client, header, body);
        this.parser = parser;
    }

    @Override
    protected RpcClientResponseHandler createHandler(CompletableFuture<RpcClientResponse<ResponseType>> result) {
        return new RpcClientResponseHandler() {
            @Override
            public void onAcknowledgement() {
                // there's nothing to do in that case
            }

            @Override
            public void onResponse(List<byte[]> attachments) {
                if (!result.isDone()) {
                    if (attachments.size() < 1 || attachments.get(0) == null) {
                        throw new IllegalStateException("Received response without a body");
                    }
                    result.complete(new LazyResponse<ResponseType>(parser, attachments.get(0),
                            new ArrayList<>(attachments.subList(1, attachments.size()))));
                }
            }

            @Override
            public void onError(Throwable error) {
                result.completeExceptionally(error);
            }
        };
    }
}
