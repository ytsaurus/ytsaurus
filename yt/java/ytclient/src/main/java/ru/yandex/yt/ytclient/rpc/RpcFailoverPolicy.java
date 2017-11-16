package ru.yandex.yt.ytclient.rpc;

/**
 * @author aozeritsky
 */
public interface RpcFailoverPolicy {
    boolean onError(RpcClientRequest request, Throwable error);
    boolean onTimeout();
    boolean randomizeDcs();
}
