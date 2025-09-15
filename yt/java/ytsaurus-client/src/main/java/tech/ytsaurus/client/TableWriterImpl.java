package tech.ytsaurus.client;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

import javax.annotation.Nullable;

import com.google.protobuf.Parser;
import tech.ytsaurus.client.request.WriteTable;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.lang.NonNullApi;
import tech.ytsaurus.lang.NonNullFields;
import tech.ytsaurus.rpcproxy.TRspWriteTable;


@NonNullApi
class TableWriterBaseImpl<T> extends RawTableWriterImpl<T, TRspWriteTable> {
    protected final WriteTable<T> req;
    @Nullable
    protected ApiServiceTransaction transaction;

    TableWriterBaseImpl(WriteTable<T> req, SerializationResolver serializationResolver) {
        super(req.getWindowSize(), req.getPacketSize(), serializationResolver, req.getSerializationContext());
        this.req = req;
    }

    public void setTransaction(ApiServiceTransaction transaction) {
        if (this.transaction != null) {
            throw new IllegalStateException("Write transaction already started");
        }
        this.transaction = transaction;
    }

    @Override
    protected Parser<TRspWriteTable> responseParser() {
        return TRspWriteTable.parser();
    }

    @Override
    public CompletableFuture<?> close() {
        return super.close()
                .thenCompose(response -> {
                    if (transaction != null && transaction.isActive()) {
                        return transaction.commit()
                                .thenApply(unused -> response);
                    }
                    return CompletableFuture.completedFuture(response);
                });
    }
}

@NonNullApi
@NonNullFields
class TableWriterImpl<T> extends TableWriterBaseImpl<T> implements TableWriter<T> {
    TableWriterImpl(WriteTable<T> req, SerializationResolver serializationResolver) {
        super(req, serializationResolver);
    }

    @SuppressWarnings("unchecked")
    public CompletableFuture<TableWriter<T>> startUpload() {
        return startUploadImpl().thenApply(writer -> (TableWriter<T>) writer);
    }

    @Override
    public boolean write(List<T> rows, TableSchema schema) throws IOException {
        return super.write(rows, schema);
    }

    @Override
    public TableSchema getSchema() {
        if (tableRowsSerializer instanceof TableRowsWireSerializer) {
            return ((TableRowsWireSerializer<?>) tableRowsSerializer).getSchema();
        }
        return TableSchema.builder().build();
    }

    @Override
    public CompletableFuture<TableSchema> getTableSchema() {
        return CompletableFuture.completedFuture(schema);
    }
}

@NonNullApi
@NonNullFields
class AsyncTableWriterImpl<T> extends TableWriterBaseImpl<T> implements AsyncWriter<T>, AsyncWriterSupport<T> {
    AsyncTableWriterImpl(WriteTable<T> req, SerializationResolver serializationResolver) {
        super(req, serializationResolver);
    }

    @SuppressWarnings("unchecked")
    public CompletableFuture<AsyncWriter<T>> startUpload() {
        return super.startUploadImpl().thenApply(writer -> (AsyncWriter<T>) writer);
    }

    @Override
    public CompletableFuture<Void> write(List<T> rows) {
        Objects.requireNonNull(tableRowsSerializer);
        TableSchema schema;
        if (req.getTableSchema().isPresent()) {
            schema = req.getTableSchema().get();
        } else if (tableRowsSerializer instanceof TableRowsWireSerializer &&
                ((TableRowsWireSerializer<?>) tableRowsSerializer).getSchema().getColumnsCount() > 0) {
            schema = ((TableRowsWireSerializer<?>) tableRowsSerializer).getSchema();
        } else {
            schema = this.schema;
        }

        return writeImpl(rows, schema);
    }
}
