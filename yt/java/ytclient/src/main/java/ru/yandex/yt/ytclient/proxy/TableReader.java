package ru.yandex.yt.ytclient.proxy;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import NYT.NChunkClient.NProto.DataStatistics;

import ru.yandex.yt.rpcproxy.TRowsetDescriptor;
import ru.yandex.yt.ytclient.tables.TableSchema;
import ru.yandex.yt.ytclient.wire.UnversionedRowset;

public interface TableReader {
    //! Returns the starting row index within the table.
    long getStartRowIndex();

    //! Returns the total (approximate) number of rows readable.
    long getTotalRowCount();

    //! Returns various data statistics.
    DataStatistics.TDataStatistics getDataStatistics();

    //! Returns schema of the table.
    TableSchema getTableSchema();

    TRowsetDescriptor getRowsetDescriptor();

    UnversionedRowset read() throws Exception;

    CompletableFuture<Void> read(Consumer<UnversionedRowset> consumer);
}
