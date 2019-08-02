package ru.yandex.yt.ytclient.proxy;

import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

// WIP
// This Api may change in the near future
public interface FileWriter {
    // send null data to close stream

    // async api
    CompletableFuture<Void> write(Supplier<byte[]> supplier);

    // sync api
    void write(byte[] data, int offset, int len);
    void close();
}
