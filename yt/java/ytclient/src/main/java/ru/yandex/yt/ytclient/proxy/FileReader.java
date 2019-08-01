package ru.yandex.yt.ytclient.proxy;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

public interface FileReader {
    CompletableFuture<Long> revision() throws Exception;

    // async
    void read(Consumer<byte[]> consumer);

    // sync
    byte[] read() throws Exception;

    CompletableFuture<Void> waitResult();

    void cancel();
}
