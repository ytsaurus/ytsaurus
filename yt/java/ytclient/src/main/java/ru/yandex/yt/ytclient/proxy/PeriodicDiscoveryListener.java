package ru.yandex.yt.ytclient.proxy;

import java.util.Set;

import ru.yandex.yt.ytclient.proxy.internal.HostPort;

public interface PeriodicDiscoveryListener {
    void onProxiesSet(Set<HostPort> proxies);
    void onError(Throwable e);
}
