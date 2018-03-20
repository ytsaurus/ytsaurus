package ru.yandex.yt.ytclient.proxy;

import ru.yandex.yt.TReqDiscoverProxies;
import ru.yandex.yt.TRspDiscoverProxies;
import ru.yandex.yt.ytclient.rpc.DiscoverableRpcService;
import ru.yandex.yt.ytclient.rpc.RpcClientRequestBuilder;
import ru.yandex.yt.ytclient.rpc.RpcClientResponse;
import ru.yandex.yt.ytclient.rpc.annotations.RpcService;

@RpcService(protocolVersion = 0)
public interface DiscoveryService extends DiscoverableRpcService {
    RpcClientRequestBuilder<TReqDiscoverProxies.Builder, RpcClientResponse<TRspDiscoverProxies>> discoverProxies();
}
