package ru.yandex.yt.ytclient.proxy;

import java.util.Set;

import ru.yandex.yt.ytclient.rpc.RpcClient;

public interface PeriodicDiscoveryListener {
    void onProxiesAdded(Set<RpcClient> proxies);
    void onProxiesRemoved(Set<RpcClient> proxies);
    void onError(Throwable e);
}
