package tech.ytsaurus.client;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import tech.ytsaurus.core.tables.TableSchema;

interface AsyncWriterSupport<T> {

    CompletableFuture<Void> readyEvent();

    boolean write(List<T> rows, TableSchema schema) throws IOException;

    default CompletableFuture<Void> writeImpl(List<T> rows, TableSchema schema) {
        try {
            if (write(rows, schema)) {
                return CompletableFuture.completedFuture(null);
            }
            return readyEvent().thenCompose(unused -> writeImpl(rows, schema));
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }
}
