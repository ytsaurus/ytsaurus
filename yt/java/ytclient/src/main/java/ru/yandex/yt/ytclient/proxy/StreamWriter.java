package ru.yandex.yt.ytclient.proxy;

import java.util.concurrent.CompletableFuture;

public interface StreamWriter {
    CompletableFuture<Void> readyEvent();
    CompletableFuture<Void> close();
}
