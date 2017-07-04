package ru.yandex.yt.ytclient.rpc;

/**
 * @author aozeritsky
 */
public class AlwaysSwitchRpcFailoverPolicy implements RpcFailoverPolicy {
    public boolean onError(RpcClientRequest request, Throwable error) {
        return true;
    }

    public boolean onTimeout() {
        return true;
    }
}
