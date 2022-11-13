package ru.yandex.yt.ytclient.proxy.internal;


import tech.ytsaurus.client.rpc.RpcClient;
public interface RpcClientFactory {
    RpcClient create(HostPort hostPort, String name);
}
