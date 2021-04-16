package ru.yandex.yt.ytclient.proxy;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import ru.yandex.yt.ytclient.proxy.internal.HostPort;
import ru.yandex.yt.ytclient.proxy.internal.RpcClientFactory;
import ru.yandex.yt.ytclient.rpc.RpcClient;
import ru.yandex.yt.ytclient.rpc.RpcClientRequestControl;
import ru.yandex.yt.ytclient.rpc.RpcClientResponseHandler;
import ru.yandex.yt.ytclient.rpc.RpcClientStreamControl;
import ru.yandex.yt.ytclient.rpc.RpcOptions;
import ru.yandex.yt.ytclient.rpc.RpcRequest;
import ru.yandex.yt.ytclient.rpc.RpcStreamConsumer;

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
            final AtomicInteger refCounter = new AtomicInteger(1);
            @Override
            public void ref() {
                int oldValue = refCounter.getAndIncrement();
                if (oldValue <= 0) {
                    throw new IllegalStateException();
                }
            }

            @Override
            public void unref() {
                int newValue = refCounter.decrementAndGet();
                if (newValue == 0) {
                    close();
                } else if (newValue < 0) {
                    throw new IllegalStateException();
                }
            }

            @Override
            public void close() {
                openedConnections.remove(hostPort);
            }

            @Override
            public RpcClientRequestControl send(RpcClient sender, RpcRequest<?> request, RpcClientResponseHandler handler, RpcOptions options) {
                return null;
            }

            @Override
            public RpcClientStreamControl startStream(RpcClient sender, RpcRequest<?> request, RpcStreamConsumer consumer, RpcOptions options) {
                return null;
            }

            @Override
            public String destinationName() {
                return hostPort.toString();
            }

            @Override
            public String getAddressString() {
                return hostPort.toString();
            }

            @Override
            public ScheduledExecutorService executor() {
                return null;
            }
        };
    }
}
