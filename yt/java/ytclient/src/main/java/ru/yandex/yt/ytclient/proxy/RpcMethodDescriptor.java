package ru.yandex.yt.ytclient.proxy;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;

import com.google.protobuf.MessageLite;
import com.google.protobuf.Parser;

import ru.yandex.inside.yt.kosher.common.GUID;
import ru.yandex.yt.rpc.TRequestHeader;
import ru.yandex.yt.rpc.TResponseHeader;
import ru.yandex.yt.tracing.TTracingExt;
import ru.yandex.yt.ytclient.rpc.RpcClient;
import ru.yandex.yt.ytclient.rpc.RpcClientRequestBuilder;
import ru.yandex.yt.ytclient.rpc.RpcClientRequestControl;
import ru.yandex.yt.ytclient.rpc.RpcClientResponse;
import ru.yandex.yt.ytclient.rpc.RpcClientResponseHandler;
import ru.yandex.yt.ytclient.rpc.RpcOptions;
import ru.yandex.yt.ytclient.rpc.RpcRequest;
import ru.yandex.yt.ytclient.rpc.RpcUtil;
import ru.yandex.yt.ytclient.rpc.internal.LazyResponse;
import ru.yandex.yt.ytclient.rpc.internal.RequestWithResponseBuilder;

public class RpcMethodDescriptor<RequestBuilder extends MessageLite.Builder, Response extends MessageLite> {
    private final int protocolVersion;
    private final String serviceName;
    private final String methodName;

    private final Supplier<RequestBuilder> requestFactory;
    private final Parser<Response> responseParser;

    public RpcMethodDescriptor(
            int protocolVersion,
            String serviceName,
            String methodName,
            Supplier<RequestBuilder> requestFactory,
            Parser<Response> responseParser
    ) {
        this.protocolVersion = protocolVersion;
        this.serviceName = serviceName;
        this.methodName = methodName;
        this.requestFactory = requestFactory;
        this.responseParser = responseParser;
    }

    public TRequestHeader.Builder createHeader(RpcOptions options) {
        TRequestHeader.Builder builder = TRequestHeader.newBuilder();
        builder.setRequestId(RpcUtil.toProto(GUID.create()));
        builder.setService(serviceName);
        builder.setMethod(methodName);
        builder.setProtocolVersionMajor(protocolVersion);
        if (options.getGlobalTimeout() != null) {
            builder.setTimeout(RpcUtil.durationToMicros(options.getGlobalTimeout()));
        }
        if (options.getTrace()) {
            TTracingExt.Builder tracing = TTracingExt.newBuilder();
            tracing.setSampled(options.getTraceSampled());
            tracing.setDebug(options.getTraceDebug());
            tracing.setTraceId(RpcUtil.toProto(GUID.create()));
            tracing.setSpanId(ThreadLocalRandom.current().nextLong());
            builder.setExtension(TRequestHeader.tracingExt, tracing.build());
        }
        return builder;
    }

    public RpcClientResponseHandler createResponseHandler(CompletableFuture<RpcClientResponse<Response>> result) {
        return new RpcClientResponseHandler() {
            @Override
            public void onResponse(RpcClient sender, TResponseHeader header, List<byte[]> attachments) {
                if (!result.isDone()) {
                    if (attachments.size() < 1 || attachments.get(0) == null) {
                        throw new IllegalStateException("Received response without a body");
                    }
                    result.complete(
                            new LazyResponse<>(
                                    responseParser,
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

    RpcClientRequestBuilder<RequestBuilder, Response> createRequestBuilder(RpcOptions options) {
        TRequestHeader.Builder header = createHeader(options);
        RequestBuilder request = requestFactory.get();

        return new RequestWithResponseBuilder<>(
                header,
                request,
                responseParser,
                options);
    }

    public CompletableFuture<RpcClientResponse<Response>> invoke(
            RpcClient client,
            RpcRequest<?> request,
            RpcOptions options
    ) {
        CompletableFuture<RpcClientResponse<Response>> result = new CompletableFuture<>();
        RpcClientResponseHandler handler = createResponseHandler(result);
        RpcClientRequestControl control = client.send(client, request, handler, options);
        result.whenComplete((ignoredResult, ignoredException) -> control.cancel());
        return result;
    }
}
