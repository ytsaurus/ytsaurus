package ru.yandex.yt.ytclient.proxy;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

public interface FileReader {
    Long revision() throws Exception;

    // async
    CompletableFuture<Void> read(Consumer<byte[]> consumer);

    // sync
    byte[] read() throws Exception;

    void cancel();
}
