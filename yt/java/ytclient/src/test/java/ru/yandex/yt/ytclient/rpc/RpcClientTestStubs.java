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
        public void ref() {
            throw new IllegalStateException("not implemented");
        }

        @Override
        public void unref() {
            throw new IllegalStateException("not implemented");
        }

        @Override
        public void close() {
            System.out.println("Close client!");
        }

        @Override
        public RpcClientStreamControl startStream(
                RpcClient sender,
                RpcRequest<?> request,
                RpcStreamConsumer consumer,
                RpcOptions options)
        {
            return null;
        }

        @Override
        public String destinationName() {
            return null;
        }

        @Override
        public String getAddressString() {
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
        public RpcClientRequestControl send(
                RpcClient sender,
                RpcRequest request,
                RpcClientResponseHandler handler,
                RpcOptions options)
        {
            return null;
        }
    }

    public static class RpcClientFactoryStub implements RpcClientFactory {
        private final Function<String, RpcClient> client;

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
