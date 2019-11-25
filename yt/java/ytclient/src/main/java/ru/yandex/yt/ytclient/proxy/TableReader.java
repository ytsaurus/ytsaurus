package ru.yandex.yt.ytclient.proxy;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import NYT.NChunkClient.NProto.DataStatistics;

import ru.yandex.yt.rpcproxy.TRowsetDescriptor;
import ru.yandex.yt.ytclient.tables.TableSchema;

public interface TableReader<T> {
    //! Returns the starting row index within the table.
    long getStartRowIndex();

    //! Returns the total (approximate) number of rows readable.
    long getTotalRowCount();

    //! Returns various data statistics.
    DataStatistics.TDataStatistics getDataStatistics();

    //! Returns schema of the table.
    TableSchema getTableSchema();

    TableSchema getCurrentReadSchema();

    List<String> getOmittedInaccessibleColumns();

    TRowsetDescriptor getRowsetDescriptor();

    CompletableFuture<Void> readyEvent();

    boolean canRead();

    List<T> read() throws Exception;

    CompletableFuture<Void> close();

    void cancel();
}
