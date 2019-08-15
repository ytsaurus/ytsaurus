package ru.yandex.yt.ytclient.proxy;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import ru.yandex.yt.rpcproxy.TRowsetDescriptor;
import ru.yandex.yt.ytclient.tables.TableSchema;
import ru.yandex.yt.ytclient.wire.UnversionedRowset;

public interface TableWriter {
    //! Attempts to write a bunch of #rows. If false is returned then the rows
    //! are not accepted and the client must invoke #GetReadyEvent and wait.
    boolean write(UnversionedRowset rows) throws IOException;

    //! Returns an asynchronous flag enabling to wait until data is written.
    CompletableFuture<Void> readyEvent();

    //! Closes the writer. Must be the last call to the writer.
    CompletableFuture<Void> close();

    //! Returns the name table to be used for constructing rows.
    TRowsetDescriptor getRowsetDescriptor();

    //! Returns the schema to be used for constructing rows.
    TableSchema getTableSchema();
}
