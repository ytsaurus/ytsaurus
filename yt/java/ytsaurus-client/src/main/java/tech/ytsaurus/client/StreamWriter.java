package tech.ytsaurus.client;

import java.util.concurrent.CompletableFuture;

public interface StreamWriter {
    CompletableFuture<?> readyEvent();

    CompletableFuture<?> close();
}
