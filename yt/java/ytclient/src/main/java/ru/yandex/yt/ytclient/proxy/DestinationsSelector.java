package ru.yandex.yt.ytclient.proxy;

import java.util.List;

import ru.yandex.yt.ytclient.rpc.RpcClient;
import ru.yandex.yt.ytclient.rpc.RpcOptions;

public abstract class DestinationsSelector extends ApiServiceClient {
    public DestinationsSelector(RpcOptions options) {
        super(options);
    }

    abstract public List<RpcClient> selectDestinations();
}
