package tech.ytsaurus.client.rpc;

import java.util.concurrent.CompletableFuture;

import tech.ytsaurus.lang.NonNullApi;
import tech.ytsaurus.lang.NonNullFields;

/**
 * Rpc client that retries requests of the wrapped client by {@link FailoverRpcExecutor}, the same executor that
 * serves clients with proxy discovering, but with a single destination and hence without failover.
 *
 * <p>
 * A response is reported with the wrapped client as its sender, so a client created from a response — a sticky
 * transaction, see {@link tech.ytsaurus.client.ApiServiceClientImpl} — does not retry its requests.
 */
@NonNullApi
@NonNullFields
public class RetryingRpcClient extends RpcClientWrapper {

    public RetryingRpcClient(RpcClient innerClient) {
        super(innerClient);
    }

    @Override
    public RpcClientRequestControl send(
            RpcClient sender,
            RpcRequest<?> request,
            RpcClientResponseHandler handler,
            RpcOptions options
    ) {
        return FailoverRpcExecutor.execute(
                executor(),
                this::peekInnerClient,
                request,
                handler,
                options,
                false);
    }

    /** The destination is fixed and is not owned by the executor, so there is nothing to release. */
    private CompletableFuture<RpcClient> peekInnerClient(CompletableFuture<?> releaseFuture) {
        return CompletableFuture.completedFuture(innerClient);
    }
}
