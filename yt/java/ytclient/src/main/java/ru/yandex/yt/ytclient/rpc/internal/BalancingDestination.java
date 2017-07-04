package ru.yandex.yt.ytclient.rpc.internal;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;

import ru.yandex.yt.rpcproxy.ETransactionType;
import ru.yandex.yt.rpcproxy.TReqPingTransaction;
import ru.yandex.yt.rpcproxy.TReqStartTransaction;
import ru.yandex.yt.rpcproxy.TRspPingTransaction;
import ru.yandex.yt.rpcproxy.TRspStartTransaction;
import ru.yandex.yt.ytclient.misc.YtGuid;
import ru.yandex.yt.ytclient.proxy.ApiService;
import ru.yandex.yt.ytclient.rpc.RpcClient;
import ru.yandex.yt.ytclient.rpc.RpcClientRequestBuilder;
import ru.yandex.yt.ytclient.rpc.RpcClientResponse;
import ru.yandex.yt.ytclient.rpc.RpcUtil;

/**
 * @author aozeritsky
 */
public class BalancingDestination {
    private final RpcClient client;
    private boolean isAlive;
    private int index;

    final ApiService service;
    YtGuid transaction = null;

    public BalancingDestination(RpcClient client, int index) {
        this.client = Objects.requireNonNull(client);
        isAlive = true;
        this.index = index;
        service = client.getService(ApiService.class);
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int i) {
        index = i;
    }

    public RpcClient getClient() {
        return client;
    }

    public void close() {
        client.close();
    }

    public CompletableFuture<YtGuid> createTransaction() {
        if (transaction == null) {
            RpcClientRequestBuilder<TReqStartTransaction.Builder, RpcClientResponse<TRspStartTransaction>> builder =
                service.startTransaction();
            builder.body().setType(ETransactionType.TABLET);
            builder.body().setSticky(true);
            return RpcUtil.apply(builder.invoke(), response -> {
                YtGuid id = YtGuid.fromProto(response.body().getId());
                return id;
            });
        } else {
            return CompletableFuture.completedFuture(transaction);
        }
    }

    public CompletableFuture<Void> pingTransaction(YtGuid id) {
        RpcClientRequestBuilder<TReqPingTransaction.Builder, RpcClientResponse<TRspPingTransaction>> builder =
            service.pingTransaction();
        builder.body().setTransactionId(id.toProto());
        builder.body().setSticky(true);
        return RpcUtil.apply(builder.invoke(), response -> null).thenAccept(unused -> {
            transaction = id;
        });
    }

    public void resetTransaction() {
        transaction = null;
    }

    public String toString() {
        return client.toString();
    }
}
