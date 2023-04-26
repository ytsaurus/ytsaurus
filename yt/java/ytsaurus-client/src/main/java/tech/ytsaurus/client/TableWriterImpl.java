package tech.ytsaurus.client;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

import javax.annotation.Nullable;

import tech.ytsaurus.client.request.WriteTable;
import tech.ytsaurus.client.rows.UnversionedRow;
import tech.ytsaurus.client.rows.UnversionedRowSerializer;
import tech.ytsaurus.client.rpc.Compression;
import tech.ytsaurus.client.rpc.RpcUtil;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.lang.NonNullApi;
import tech.ytsaurus.lang.NonNullFields;
import tech.ytsaurus.rpcproxy.TWriteTableMeta;


@NonNullApi
class TableWriterBaseImpl<T> extends RawTableWriterImpl {
    protected @Nullable
    TableSchema schema;
    protected final WriteTable<T> req;
    protected @Nullable
    TableRowsSerializer<T> tableRowsSerializer;
    private final SerializationResolver serializationResolver;
    @Nullable
    protected ApiServiceTransaction transaction;

    TableWriterBaseImpl(WriteTable<T> req, SerializationResolver serializationResolver) {
        super(req.getWindowSize(), req.getPacketSize());
        this.req = req;
        this.serializationResolver = serializationResolver;
        this.tableRowsSerializer = TableRowsSerializer.createTableRowsSerializer(
                this.req.getSerializationContext(), serializationResolver).orElse(null);
    }

    public void setTransaction(ApiServiceTransaction transaction) {
        if (this.transaction != null) {
            throw new IllegalStateException("Write transaction already started");
        }
        this.transaction = transaction;
    }

    public CompletableFuture<TableWriterBaseImpl<T>> startUploadImpl() {
        TableWriterBaseImpl<T> self = this;

        return startUpload.thenApply((attachments) -> {
            if (attachments.size() != 1) {
                throw new IllegalArgumentException("protocol error");
            }
            byte[] head = attachments.get(0);
            if (head == null) {
                throw new IllegalArgumentException("protocol error");
            }

            TWriteTableMeta metadata = RpcUtil.parseMessageBodyWithCompression(
                    head,
                    TWriteTableMeta.parser(),
                    Compression.None
            );
            self.schema = ApiServiceUtil.deserializeTableSchema(metadata.getSchema());
            logger.debug("schema -> {}", schema.toYTree().toString());

            if (this.tableRowsSerializer == null) {
                if (this.req.getSerializationContext().getObjectClass().isEmpty()) {
                    throw new IllegalStateException("No object clazz");
                }
                Class<T> objectClazz = self.req.getSerializationContext().getObjectClass().get();
                if (UnversionedRow.class.equals(objectClazz)) {
                    this.tableRowsSerializer =
                            (TableRowsSerializer<T>) new TableRowsWireSerializer<>(new UnversionedRowSerializer());
                } else {
                    this.tableRowsSerializer = new TableRowsWireSerializer<>(
                            serializationResolver.createWireRowSerializer(
                                    serializationResolver.forClass(objectClazz, self.schema))
                    );
                }
            }

            return self;
        });
    }

    public boolean write(List<T> rows, TableSchema schema) throws IOException {
        byte[] serializedRows = tableRowsSerializer.serializeRows(rows, schema);
        return write(serializedRows);
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
        return tableRowsSerializer.getSchema();
    }

    @Override
    public CompletableFuture<TableSchema> getTableSchema() {
        return CompletableFuture.completedFuture(schema);
    }
}

@NonNullApi
@NonNullFields
class AsyncTableWriterImpl<T> extends TableWriterBaseImpl<T> implements AsyncWriter<T> {
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
        } else if (tableRowsSerializer.getSchema().getColumnsCount() > 0) {
            schema = tableRowsSerializer.getSchema();
        } else {
            schema = this.schema;
        }

        return writeImpl(rows, schema);
    }

    private CompletableFuture<Void> writeImpl(List<T> rows, TableSchema schema) {
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
