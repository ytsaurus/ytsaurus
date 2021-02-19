package ru.yandex.yt.ytclient.rpc;

/**
 * @author aozeritsky
 */
public interface RpcFailoverPolicy {
    boolean onError(Throwable error);
    boolean onTimeout();
    boolean randomizeDcs();
}
