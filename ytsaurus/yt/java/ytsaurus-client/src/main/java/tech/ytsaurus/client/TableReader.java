package tech.ytsaurus.client;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import NYT.NChunkClient.NProto.DataStatistics;
import tech.ytsaurus.core.tables.TableSchema;

/**
 * Prefer to use {@link AsyncReader} instead of it. See {@link ApiServiceClient#readTableV2}.
 * @param <T> row type.
 */
public interface TableReader<T> {
    /**
     * Returns the starting row index within the table.
     */
    long getStartRowIndex();

    /**
     * Returns the total (approximate) number of rows readable.
     */
    long getTotalRowCount();

    /**
     * Returns various data statistics.
     */
    DataStatistics.TDataStatistics getDataStatistics();

    /**
     * Returns schema of the table.
     */
    TableSchema getTableSchema();

    TableSchema getCurrentReadSchema();

    List<String> getOmittedInaccessibleColumns();

    CompletableFuture<Void> readyEvent();

    boolean canRead();

    List<T> read() throws Exception;

    CompletableFuture<Void> close();

    void cancel();
}
