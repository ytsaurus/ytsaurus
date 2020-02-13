package ru.yandex.yt.ytclient.proxy.internal;

import ru.yandex.yt.ytclient.rpc.RpcClient;

public interface RpcClientFactory {
    RpcClient create(HostPort hostPort, String name);
}
