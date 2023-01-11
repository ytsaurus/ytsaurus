package tech.ytsaurus.client.rpc;

import java.util.concurrent.ScheduledExecutorService;

public class RpcClientTestStubs {
    public abstract static class RpcClientStub implements RpcClient {
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
                RpcOptions options
        ) {
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
                RpcOptions options
        ) {
            return null;
        }
    }
}
