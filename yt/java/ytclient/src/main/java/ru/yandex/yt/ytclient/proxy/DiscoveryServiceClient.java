package ru.yandex.yt.ytclient.proxy;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import ru.yandex.yt.TReqDiscoverProxies;
import ru.yandex.yt.TRspDiscoverProxies;
import ru.yandex.yt.ytclient.rpc.RpcClient;
import ru.yandex.yt.ytclient.rpc.RpcClientRequestBuilder;
import ru.yandex.yt.ytclient.rpc.RpcClientResponse;
import ru.yandex.yt.ytclient.rpc.RpcOptions;
import ru.yandex.yt.ytclient.rpc.RpcUtil;
import ru.yandex.yt.ytclient.rpc.internal.RpcServiceClient;

public class DiscoveryServiceClient {
    private final DiscoveryService service;
    private final RpcClient client;

    public DiscoveryServiceClient(RpcClient client, RpcOptions options) {
        this.client = client;
        this.service = RpcServiceClient.create(DiscoveryService.class, options);
    }

    public RpcClient getClient() {
        return client;
    }

    public CompletableFuture<List<String>> discoverProxies(String role) {
        RpcClientRequestBuilder<TReqDiscoverProxies.Builder, RpcClientResponse<TRspDiscoverProxies>> builder =
                service.discoverProxies();
        if (role != null) {
            builder.body().setRole(role);
        }
        return RpcUtil.apply(builder.invoke(client), response -> response.body().getAddressesList());
    }
}
