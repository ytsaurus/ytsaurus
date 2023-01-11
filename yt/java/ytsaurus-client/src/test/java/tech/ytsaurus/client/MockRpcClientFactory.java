package tech.ytsaurus.client;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import tech.ytsaurus.client.rpc.RpcClient;
import tech.ytsaurus.client.rpc.RpcClientRequestControl;
import tech.ytsaurus.client.rpc.RpcClientResponseHandler;
import tech.ytsaurus.client.rpc.RpcClientStreamControl;
import tech.ytsaurus.client.rpc.RpcOptions;
import tech.ytsaurus.client.rpc.RpcRequest;
import tech.ytsaurus.client.rpc.RpcStreamConsumer;

class MockRpcClientFactory implements RpcClientFactory, SelfCheckingClientFactory {
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
            public RpcClientRequestControl send(
                    RpcClient sender, RpcRequest<?> request, RpcClientResponseHandler handler, RpcOptions options) {
                return null;
            }

            @Override
            public RpcClientStreamControl startStream(
                    RpcClient sender, RpcRequest<?> request, RpcStreamConsumer consumer, RpcOptions options) {
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

    @Override
    public RpcClient create(HostPort hostPort, String name, CompletableFuture<Void> statusFuture) {
        return create(hostPort, name);
    }
}
