package ru.yandex.yt.ytclient.rpc;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import ru.yandex.yt.ytclient.rpc.internal.Compression;

public interface RpcClientStreamControl extends RpcClientRequestControl {
    void subscribe(RpcStreamConsumer consumer);

    Compression compression();

    CompletableFuture<Void> feedback(long offset);
    CompletableFuture<Void> sendEof();

    CompletableFuture<Void> sendPayload(List<byte[]> attachments);

    void wakeUp();
}
