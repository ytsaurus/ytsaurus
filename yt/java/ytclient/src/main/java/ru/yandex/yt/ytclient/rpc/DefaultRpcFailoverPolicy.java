package ru.yandex.yt.ytclient.rpc;

/**
 * Created by aozeritsky on 28.06.2017.
 */
public class DefaultRpcFailoverPolicy implements RpcFailoverPolicy {
    public boolean onError(RpcClientRequest request, Throwable error) {
        return false;
    }

    public boolean onTimeout() {
        return true;
    }
}
