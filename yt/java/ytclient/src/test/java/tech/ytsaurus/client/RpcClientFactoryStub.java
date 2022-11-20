package tech.ytsaurus.client;

import java.util.function.Function;

import tech.ytsaurus.client.rpc.RpcClient;
import tech.ytsaurus.client.rpc.RpcClientTestStubs;

public class RpcClientFactoryStub implements RpcClientFactory {
    private final Function<String, RpcClient> client;

    public RpcClientFactoryStub() {
        this.client = RpcClientTestStubs.RpcClientStubImpl::new;
    }

    public RpcClientFactoryStub(Function<String, RpcClient> client) {
        this.client = client;
    }

    @Override
    public RpcClient create(HostPort hostPort, String name) {
        return client.apply(name + "/" + hostPort.getPort());
    }
}
