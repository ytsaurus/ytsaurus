package ru.yandex.yt.ytclient.proxy;

import java.util.concurrent.CompletableFuture;

public interface FileWriter {
    //! Returns an asynchronous flag enabling to wait until data is written.
    CompletableFuture<Void> readyEvent();

    //! Attempts to write a bunch of #rows. If false is returned then the rows
    //! are not accepted and the client must invoke #GetReadyEvent and wait.
    boolean write(byte[] data, int offset, int len);

    //! Closes the writer. Must be the last call to the writer.
    CompletableFuture<Void> close();

    void cancel();
}
