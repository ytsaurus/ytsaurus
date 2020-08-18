package ru.yandex.yt.ytclient.proxy;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;
import ru.yandex.yt.ytclient.rpc.RpcClient;
import ru.yandex.yt.ytclient.rpc.RpcClientPool;

/**
 * Miscellaneous stuff to help with backward compatibility.
 * Should not be used in new code.
 * @deprecated
 */
@Deprecated
@NonNullFields
@NonNullApi
public class BackwardCompatibility {
    private BackwardCompatibility() {}

    /**
     * Get list of rpcclients that can be used to send messages to YtClient.
     */
    static public List<RpcClient> selectDestinations(YtClient client) {
        final int RESULT_SIZE = 3;
        final CompletableFuture<Void> releaseFuture = new CompletableFuture<>();

        RpcClientPool clientPool = client.getClientPool();
        List<RpcClient> result = new ArrayList<>();
        try {
            for (int i = 0; i < RESULT_SIZE; ++i) {
                CompletableFuture<RpcClient> clientFuture = clientPool.peekClient(releaseFuture);
                try {
                    result.add(clientFuture.get());
                } catch (ExecutionException | InterruptedException error) {
                    if (result.isEmpty()) {
                        throw new RuntimeException(error);
                    } else {
                        break;
                    }
                }
            }
        } finally {
            releaseFuture.complete(null);
        }
        return result;
    }
}
