package tech.ytsaurus.client;

import tech.ytsaurus.client.rpc.RpcClient;

interface RpcClientFactory {
    RpcClient create(HostPort hostPort, String name);
}
