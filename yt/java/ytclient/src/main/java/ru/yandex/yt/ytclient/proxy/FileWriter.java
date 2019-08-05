package ru.yandex.yt.ytclient.proxy;

import java.util.concurrent.CompletableFuture;

public interface FileWriter {
    CompletableFuture<Void> readyEvent();

    void write(byte[] data, int offset, int len);

    CompletableFuture<Void> close();
}
