package tech.ytsaurus.client;

import java.util.concurrent.CompletableFuture;

public interface FileReader {
    /**
     * Returns revision of file node.
     */
    long revision() throws Exception;

    CompletableFuture<Void> readyEvent();

    boolean canRead() throws Exception;

    byte[] read() throws Exception;

    CompletableFuture<Void> close();
}
