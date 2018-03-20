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

public class DiscoveryServiceClient {
    private final DiscoveryService service;

    public DiscoveryServiceClient(RpcClient client, RpcOptions options) {
        this.service = client.getService(DiscoveryService.class, options);
    }

    public CompletableFuture<List<String>> discoverProxies() {
        RpcClientRequestBuilder<TReqDiscoverProxies.Builder, RpcClientResponse<TRspDiscoverProxies>> builder =
                service.discoverProxies();
        return RpcUtil.apply(builder.invoke(), response -> response.body().getAddressesList());
    }
}
