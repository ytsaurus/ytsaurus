package tech.ytsaurus.client.rpc;

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;


import tech.ytsaurus.lang.NonNullApi;
@NonNullApi
public interface RpcClientPool {
    /**
     * Get the client from the pool.
     *
     * @param releaseFuture is used by pool to track if returned client is still in use;
     *                      once releaseFuture is done (successfully or erroneously)
     *                      pool considers that user don't need RpcClient anymore;
     *                      RpcClient might then be closed and released if pool decides it is appropriate.
     */
    CompletableFuture<RpcClient> peekClient(CompletableFuture<?> releaseFuture);

    static RpcClientPool collectionPool(Collection<RpcClient> clients) {
        return new IteratorPool(clients.iterator());
    }

    static RpcClientPool collectionPool(Stream<RpcClient> clients) {
        return new IteratorPool(clients.iterator());
    }
}

@NonNullApi
class IteratorPool implements RpcClientPool {
    final Iterator<RpcClient> iterator;

    IteratorPool(Iterator<RpcClient> iterator) {
        this.iterator = iterator;
    }

    @Override
    public CompletableFuture<RpcClient> peekClient(CompletableFuture<?> releaseFuture) {
        if (iterator.hasNext()) {
            return CompletableFuture.completedFuture(iterator.next());
        } else {
            return RpcUtil.failedFuture(new Exception("RpcClient pool is exhausted"));
        }
    }
}

