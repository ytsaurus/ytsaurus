package tech.ytsaurus.client.rpc;

import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;


import tech.ytsaurus.lang.NonNullFields;
/**
 * Base class for all rpc-clients that wrap other rpc clients overriding some of its methods.
 * Current implementation just calls methods of innerClient.
 */
@NonNullFields
public class RpcClientWrapper implements RpcClient {
    protected final RpcClient innerClient;

    public RpcClientWrapper(@Nonnull RpcClient innerClient) {
        this.innerClient = Objects.requireNonNull(innerClient);
    }

    @Override
    public void ref() {
        innerClient.ref();
    }

    @Override
    public void unref() {
        innerClient.unref();
    }

    @Override
    public void close() {
        innerClient.close();
    }

    @Override
    public RpcClientRequestControl send(
            RpcClient sender,
            RpcRequest<?> request,
            RpcClientResponseHandler handler,
            RpcOptions options
    ) {
        return innerClient.send(sender, request, handler, options);
    }

    @Override
    public RpcClientStreamControl startStream(
            RpcClient sender,
            RpcRequest<?> request,
            RpcStreamConsumer consumer,
            RpcOptions options
    ) {
        return innerClient.startStream(sender, request, consumer, options);
    }

    @Override
    public String destinationName() {
        return innerClient.destinationName();
    }

    @Override
    public ScheduledExecutorService executor() {
        return innerClient.executor();
    }

    @Nullable
    @Override
    public String getAddressString() {
        return innerClient.getAddressString();
    }

    @Override
    public String toString() {
        return innerClient.toString();
    }
}
