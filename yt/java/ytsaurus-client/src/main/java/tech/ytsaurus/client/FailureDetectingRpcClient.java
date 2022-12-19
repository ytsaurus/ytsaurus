package tech.ytsaurus.client;

import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.function.Consumer;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ytsaurus.client.rpc.RpcClient;
import tech.ytsaurus.client.rpc.RpcClientRequestControl;
import tech.ytsaurus.client.rpc.RpcClientResponseHandler;
import tech.ytsaurus.client.rpc.RpcClientStreamControl;
import tech.ytsaurus.client.rpc.RpcClientWrapper;
import tech.ytsaurus.client.rpc.RpcOptions;
import tech.ytsaurus.client.rpc.RpcRequest;
import tech.ytsaurus.client.rpc.RpcStreamConsumer;
import tech.ytsaurus.rpc.TResponseHeader;

// TODO: move closer to user an make package private
class FailureDetectingRpcClient extends RpcClientWrapper {
    private static final Logger logger = LoggerFactory.getLogger(FailureDetectingRpcClient.class);

    private Function<Throwable, Boolean> isError;
    private Consumer<Throwable> errorHandler;

    FailureDetectingRpcClient(
            RpcClient innerClient,
            Function<Throwable, Boolean> isError,
            Consumer<Throwable> errorHandler
    ) {
        super(innerClient);
        setHandlers(isError, errorHandler);
    }

    protected FailureDetectingRpcClient(RpcClient innerClient) {
        super(innerClient);
    }

    protected void setHandlers(Function<Throwable, Boolean> isError, Consumer<Throwable> errorHandler) {
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
    public RpcClientRequestControl send(
            RpcClient sender,
            RpcRequest<?> request,
            RpcClientResponseHandler handler,
            RpcOptions options
    ) {
        return super.send(sender, request, wrapHandler(handler), options);
    }

    @Override
    public RpcClientStreamControl startStream(
            RpcClient sender,
            RpcRequest<?> request,
            RpcStreamConsumer consumer,
            RpcOptions options
    ) {
        return super.startStream(sender, request, consumer, options);
    }
}
