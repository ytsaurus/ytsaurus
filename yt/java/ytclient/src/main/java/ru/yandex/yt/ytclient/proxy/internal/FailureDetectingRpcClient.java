package ru.yandex.yt.ytclient.proxy.internal;

import java.util.List;
import java.util.concurrent.CancellationException;
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
import ru.yandex.yt.ytclient.rpc.RpcClientWrapper;
import ru.yandex.yt.ytclient.rpc.RpcStreamConsumer;

// TODO: move closer to user an make package private
public class FailureDetectingRpcClient extends RpcClientWrapper {
    private static final Logger logger = LoggerFactory.getLogger(FailureDetectingRpcClient.class);

    private final Function<Throwable, Boolean> isError;
    private final Consumer<Throwable> errorHandler;

    public FailureDetectingRpcClient(
            RpcClient innerClient,
            Function<Throwable, Boolean> isError,
            Consumer<Throwable> errorHandler)
    {
        super(innerClient);
        this.isError = isError;
        this.errorHandler = errorHandler;
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
                    logger.error("Unrecoverable error in RPC response: {}", error.toString());
                    errorHandler.accept(error);
                } else {
                    logger.info("Error in RPC response: {}", error.toString());
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
        return super.send(sender, request, wrapHandler(handler));
    }

    @Override
    public RpcClientRequestControl send(RpcClientRequest request, RpcClientResponseHandler handler) {
        return super.send(this, request, wrapHandler(handler));
    }

    @Override
    public RpcClientStreamControl startStream(RpcClient sender, RpcClientRequest request, RpcStreamConsumer consumer) {
        return super.startStream(sender, request, consumer);
    }
}
