package ru.yandex.yt.ytclient.proxy;

import ru.yandex.yt.ytclient.bus.BusConnector;
import ru.yandex.yt.ytclient.proxy.internal.HostPort;
import ru.yandex.yt.ytclient.proxy.internal.RpcClientFactory;
import ru.yandex.yt.ytclient.rpc.DefaultRpcBusClient;
import ru.yandex.yt.ytclient.rpc.RpcClient;
import ru.yandex.yt.ytclient.rpc.RpcCompression;
import ru.yandex.yt.ytclient.rpc.RpcCredentials;

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
