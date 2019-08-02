package ru.yandex.yt.ytclient.proxy;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

// WIP: This Api may change in the near future
public interface FileReader {
    long revision() throws Exception;

    // async
    CompletableFuture<Void> read(Consumer<byte[]> consumer);

    // sync
    byte[] read() throws Exception;

    void cancel();
}
