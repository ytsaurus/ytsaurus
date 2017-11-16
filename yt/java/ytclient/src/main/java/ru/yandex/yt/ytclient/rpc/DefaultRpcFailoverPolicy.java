package ru.yandex.yt.ytclient.rpc;

/**
 * @author aozeritsky
 */
public class DefaultRpcFailoverPolicy implements RpcFailoverPolicy {
    @Override
    public boolean onError(RpcClientRequest request, Throwable error) {
        return false;
    }

    @Override
    public boolean onTimeout() {
        return true;
    }

    @Override
    public boolean randomizeDcs() {
        return false;
    }
}
