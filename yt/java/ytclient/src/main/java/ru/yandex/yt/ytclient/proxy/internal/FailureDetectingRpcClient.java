package ru.yandex.yt.ytclient.proxy.internal;

import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.yandex.yt.rpc.TResponseHeader;
import ru.yandex.yt.ytclient.rpc.RpcClient;
import ru.yandex.yt.ytclient.rpc.RpcClientRequest;
import ru.yandex.yt.ytclient.rpc.RpcClientRequestControl;
import ru.yandex.yt.ytclient.rpc.RpcClientResponseHandler;
import ru.yandex.yt.ytclient.rpc.RpcClientStreamControl;
import ru.yandex.yt.ytclient.rpc.RpcCompression;
import ru.yandex.yt.ytclient.rpc.RpcCredentials;

// TODO: move closer to user an make package private
public class FailureDetectingRpcClient implements RpcClient {
    private static final Logger logger = LoggerFactory.getLogger(FailureDetectingRpcClient.class);

    private RpcClient innerClient;
    private final Function<Throwable, Boolean> isError;
    private final Consumer<Throwable> errorHandler;

    public FailureDetectingRpcClient(
            RpcClient innerClient,
            Function<Throwable, Boolean> isError,
            Consumer<Throwable> errorHandler)
    {
        this.innerClient = innerClient;
        this.isError = isError;
        this.errorHandler = errorHandler;
    }

    @Override
    public void close() {
        innerClient.close();
    }

    private RpcClientResponseHandler wrapHandler(RpcClientResponseHandler handler) {
        return new RpcClientResponseHandler() {
            @Override
            public void onResponse(RpcClient sender, TResponseHeader header, List<byte[]> attachments) {
                handler.onResponse(sender, header, attachments);
            }

            @Override
            public void onError(Throwable error) {
                if (isError.apply(error)) {
                    logger.error("Unrecoverable error in RPC response", error);
                    errorHandler.accept(error);
                } else {
                    logger.info("Error in RPC response", error);
                }
                handler.onError(error);
            }

            @Override
            public void onCancel(CancellationException cancel) {
                logger.debug("RPC request cancelled");
                handler.onCancel(cancel);
            }
        };
    }

    @Override
    public RpcClientRequestControl send(RpcClient sender, RpcClientRequest request, RpcClientResponseHandler handler) {
        return innerClient.send(sender, request, wrapHandler(handler));
    }

    @Override
    public RpcClientRequestControl send(RpcClientRequest request, RpcClientResponseHandler handler) {
        return innerClient.send(this, request, wrapHandler(handler));
    }

    @Override
    public RpcClientStreamControl startStream(RpcClient sender, RpcClientRequest request) {
        return innerClient.startStream(sender, request);
    }

    @Override
    public String destinationName() {
        return innerClient.destinationName();
    }

    @Override
    public ScheduledExecutorService executor() {
        return innerClient.executor();
    }

    @Override
    public RpcClientStreamControl startStream(RpcClientRequest request) {
        return innerClient.startStream(this, request);
    }

    @Override
    public RpcClient withTokenAuthentication(RpcCredentials credentials) {
        this.innerClient = innerClient.withTokenAuthentication(credentials);
        return this;
    }

    @Override
    public RpcClient withCompression(RpcCompression compression) {
        this.innerClient = innerClient.withCompression(compression);
        return this;
    }

    @Override
    public String toString() {
        return innerClient.toString();
    }
}
