package ru.yandex.yt.ytclient.rpc;

/**
 * @author aozeritsky
 */
public class DefaultRpcFailoverPolicy implements RpcFailoverPolicy {
    public boolean onError(RpcClientRequest request, Throwable error) {
        return false;
    }

    public boolean onTimeout() {
        return true;
    }
}
