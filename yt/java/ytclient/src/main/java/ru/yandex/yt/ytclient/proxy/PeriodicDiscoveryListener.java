package ru.yandex.yt.ytclient.proxy;

import java.util.List;

import ru.yandex.yt.ytclient.rpc.RpcClient;

public interface PeriodicDiscoveryListener {
    void onProxiesAdded(List<RpcClient> proxies);
    void onProxiesRemoved(List<RpcClient> proxies);
}
