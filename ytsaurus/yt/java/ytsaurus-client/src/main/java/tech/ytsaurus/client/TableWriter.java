package tech.ytsaurus.client;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;


import tech.ytsaurus.core.tables.TableSchema;

/**
 * Prefer to use {@link AsyncWriter} instead of it. See {@link ApiServiceClient#writeTableV2}.
 * @param <T> row type.
 */
public interface TableWriter<T> {
    TableSchema getSchema();

    /**
     * Attempts to write a bunch of #rows. If false is returned then the rows
     * are not accepted and the client must invoke {@link #readyEvent} and wait.
     */
    boolean write(List<T> rows, TableSchema schema) throws IOException;

    default boolean write(List<T> rows) throws IOException {
        return write(rows, getSchema());
    }

    /**
     * Returns an asynchronous flag enabling to wait until data is written.
     */
    CompletableFuture<Void> readyEvent();

    /**
     * Closes the writer. Must be the last call to the writer.
     */
    CompletableFuture<?> close();

    /**
     * Returns the schema to be used for constructing rows.
     */
    CompletableFuture<TableSchema> getTableSchema();

    void cancel();
}
