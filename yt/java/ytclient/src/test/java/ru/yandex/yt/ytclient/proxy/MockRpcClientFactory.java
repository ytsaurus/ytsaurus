package ru.yandex.yt.ytclient.proxy;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;

import ru.yandex.yt.ytclient.proxy.internal.HostPort;
import ru.yandex.yt.ytclient.proxy.internal.RpcClientFactory;
import ru.yandex.yt.ytclient.rpc.RpcClient;
import ru.yandex.yt.ytclient.rpc.RpcClientRequest;
import ru.yandex.yt.ytclient.rpc.RpcClientRequestControl;
import ru.yandex.yt.ytclient.rpc.RpcClientResponseHandler;
import ru.yandex.yt.ytclient.rpc.RpcClientStreamControl;

class MockRpcClientFactory implements RpcClientFactory {
    Set<HostPort> openedConnections = new HashSet<>();

    boolean isConnectionOpened(String hostPort) {
        return openedConnections.contains(HostPort.parse(hostPort));
    }

    @Override
    public RpcClient create(HostPort hostPort, String name) {
        if (openedConnections.contains(hostPort)) {
            throw new RuntimeException("Connection to " + hostPort + " is already opened");
        }
        openedConnections.add(hostPort);

        return new RpcClient() {
            @Override
            public void close() {
                openedConnections.remove(hostPort);
            }

            @Override
            public RpcClientRequestControl send(RpcClient sender, RpcClientRequest request, RpcClientResponseHandler handler) {
                return null;
            }

            @Override
            public RpcClientStreamControl startStream(RpcClient sender, RpcClientRequest request) {
                return null;
            }

            @Override
            public String destinationName() {
                return hostPort.toString();
            }

            @Override
            public ScheduledExecutorService executor() {
                return null;
            }
        };
    }
}
