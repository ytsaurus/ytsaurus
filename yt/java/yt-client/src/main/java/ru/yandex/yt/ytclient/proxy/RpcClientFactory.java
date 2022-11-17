package ru.yandex.yt.ytclient.proxy;

import tech.ytsaurus.client.rpc.RpcClient;

interface RpcClientFactory {
    RpcClient create(HostPort hostPort, String name);
}
