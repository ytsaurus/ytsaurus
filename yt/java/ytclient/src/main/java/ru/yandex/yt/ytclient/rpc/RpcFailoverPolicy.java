package ru.yandex.yt.ytclient.rpc;

/**
 * Created by aozeritsky on 28.06.2017.
 */
public interface RpcFailoverPolicy {
    boolean onError(RpcClientRequest request, Throwable error);
    boolean onTimeout();
}
