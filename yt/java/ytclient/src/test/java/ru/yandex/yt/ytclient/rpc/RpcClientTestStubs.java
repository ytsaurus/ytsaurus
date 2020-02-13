package ru.yandex.yt.ytclient.rpc;

import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;

import ru.yandex.yt.ytclient.proxy.internal.HostPort;
import ru.yandex.yt.ytclient.proxy.internal.RpcClientFactory;

public class RpcClientTestStubs {
    public static abstract class RpcClientStub implements RpcClient {
        private final String name;

        public RpcClientStub(String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return name;
        }

        @Override
        public void close() {
            System.out.println("Close client!");
        }

        @Override
        public RpcClientStreamControl startStream(RpcClient sender, RpcClientRequest request) {
            return null;
        }

        @Override
        public String destinationName() {
            return null;
        }

        @Override
        public ScheduledExecutorService executor() {
            return null;
        }
    }

    public static class RpcClientStubImpl extends RpcClientStub {
        public RpcClientStubImpl(String name) {
            super(name);
        }

        @Override
        public RpcClientRequestControl send(RpcClient sender, RpcClientRequest request, RpcClientResponseHandler handler) {
            return null;
        }
    }

    public static class RpcClientFactoryStub implements RpcClientFactory {
        private Function<String, RpcClient> client;

        public RpcClientFactoryStub() {
            this.client = RpcClientStubImpl::new;
        }

        public RpcClientFactoryStub(Function<String, RpcClient> client) {
            this.client = client;
        }

        @Override
        public RpcClient create(HostPort hostPort, String name) {
            return client.apply(name + "/" + hostPort.getPort());
        }
    }
}
