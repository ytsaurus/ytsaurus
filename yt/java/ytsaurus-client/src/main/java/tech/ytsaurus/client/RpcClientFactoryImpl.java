package tech.ytsaurus.client;

import tech.ytsaurus.client.bus.BusConnector;
import tech.ytsaurus.client.rpc.DefaultRpcBusClient;
import tech.ytsaurus.client.rpc.RpcClient;
import tech.ytsaurus.client.rpc.RpcCompression;
import tech.ytsaurus.client.rpc.RpcCredentials;


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
        if (!credentials.isEmpty() || credentials.getServiceTicketAuth().isPresent()) {
            rpcClient = rpcClient.withAuthentication(credentials);
        }
        return rpcClient;
    }
}
