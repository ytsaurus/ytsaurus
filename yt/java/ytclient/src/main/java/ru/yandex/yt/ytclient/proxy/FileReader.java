package ru.yandex.yt.ytclient.proxy;

import java.util.concurrent.CompletableFuture;

public interface FileReader {
    long revision() throws Exception;

    CompletableFuture<Void> readyEvent();

    byte[] read() throws Exception;

    CompletableFuture<Void> close();
}
