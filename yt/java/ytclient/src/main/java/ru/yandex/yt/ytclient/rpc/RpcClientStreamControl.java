package ru.yandex.yt.ytclient.rpc;

public interface RpcClientStreamControl extends RpcClientRequestControl {
    void subscribe(RpcStreamConsumer consumer);

    void feedback(long offset);
    void sendEof();
}
