package ru.yandex.yt.ytclient.proxy;

import tech.ytsaurus.client.rpc.DefaultRpcBusClient;
import tech.ytsaurus.client.rpc.RpcClient;
import tech.ytsaurus.client.rpc.RpcCompression;
import tech.ytsaurus.client.rpc.RpcCredentials;

import ru.yandex.yt.ytclient.bus.BusConnector;

class RpcClientFactoryImpl implements RpcClientFactory {
    private final BusConnector connector;
    private final RpcCredentials credentials;
    private final RpcCompression compression;

    RpcClientFactoryImpl(
            BusConnector connector,
            RpcCredentials credentials,
            RpcCompression compression
    ) {
        this.connector = connector;
        this.credentials = credentials;
        this.compression = compression;
    }

    @Override
    public RpcClient create(HostPort hostPort, String name) {
        RpcClient rpcClient = new DefaultRpcBusClient(connector, hostPort.toInetSocketAddress(), name);
        if (!compression.isEmpty()) {
            rpcClient = rpcClient.withCompression(compression);
        }
        if (!credentials.isEmpty()) {
            rpcClient = rpcClient.withTokenAuthentication(credentials);
        }
        return rpcClient;
    }
}
