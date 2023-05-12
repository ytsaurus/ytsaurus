package tech.ytsaurus.client;

import tech.ytsaurus.client.bus.BusConnector;
import tech.ytsaurus.client.rpc.DefaultRpcBusClient;
import tech.ytsaurus.client.rpc.RpcClient;
import tech.ytsaurus.client.rpc.RpcCompression;
import tech.ytsaurus.client.rpc.YTsaurusClientAuth;


class RpcClientFactoryImpl implements RpcClientFactory {
    private final BusConnector connector;
    private final YTsaurusClientAuth auth;
    private final RpcCompression compression;

    RpcClientFactoryImpl(
            BusConnector connector,
            YTsaurusClientAuth auth,
            RpcCompression compression
    ) {
        this.connector = connector;
        this.auth = auth;
        this.compression = compression;
    }

    @Override
    public RpcClient create(HostPort hostPort, String name) {
        RpcClient rpcClient = new DefaultRpcBusClient(connector, hostPort.toInetSocketAddress(), name);
        if (!compression.isEmpty()) {
            rpcClient = rpcClient.withCompression(compression);
        }
        if (auth.getToken().isPresent() ||
                auth.getServiceTicketAuth().isPresent() ||
                auth.getUserTicketAuth().isPresent()) {
            rpcClient = rpcClient.withAuthentication(auth);
        }
        return rpcClient;
    }
}
