package ru.yandex.yt.ytclient.proxy;

import java.io.Closeable;

import ru.yandex.yt.ytclient.rpc.RpcClientPool;
import ru.yandex.yt.ytclient.rpc.RpcOptions;

public abstract class DestinationsSelector extends ApiServiceClient implements Closeable {
    public DestinationsSelector(RpcOptions options) {
        super(options);
    }

    abstract public RpcClientPool getClientPool();
}
