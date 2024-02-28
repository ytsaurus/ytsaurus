package tech.ytsaurus.client;

import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import javax.annotation.Nullable;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ytsaurus.client.request.CreateNode;
import tech.ytsaurus.client.request.GetNode;
import tech.ytsaurus.client.request.LockNode;
import tech.ytsaurus.client.request.StartTransaction;
import tech.ytsaurus.client.request.WriteTable;
import tech.ytsaurus.client.rows.EntityTableSchemaCreator;
import tech.ytsaurus.client.rows.UnversionedRow;
import tech.ytsaurus.client.rows.UnversionedRowSerializer;
import tech.ytsaurus.client.rpc.RpcOptions;
import tech.ytsaurus.client.rpc.RpcUtil;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.core.cypress.CypressNodeType;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.core.request.LockMode;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.lang.NonNullApi;
import tech.ytsaurus.lang.NonNullFields;
import tech.ytsaurus.ysontree.YTreeNode;

@NonNullApi
@NonNullFields
class WriteTask<T> {
    final byte[] data;
    final RetryPolicy retryPolicy;
    final CompletableFuture<Void> handled;
    final int index;

    WriteTask(Buffer<T> buffer, TableRowsSerializer<T> rowsSerializer, RetryPolicy retryPolicy, int index) {
        this.data = buffer.finish(rowsSerializer);
        this.retryPolicy = retryPolicy;
        this.handled = buffer.handled;
        this.index = index;
    }
}

@NonNullApi
@NonNullFields
class Buffer<T> {
    final ByteBuf buffer;
    final CompletableFuture<Void> handled = new CompletableFuture<>();
    int rowsCount = 0;

    Buffer() {
        this.buffer = Unpooled.buffer();
    }

    public int size() {
        return buffer.readableBytes();
    }

    public void write(ByteBuf buf) {
        buffer.writeBytes(buf);
    }

    public byte[] finish(TableRowsSerializer<T> rowsSerializer) {
        try {
            return rowsSerializer.serializeRowsWithDescriptor(buffer, rowsCount);
        } catch (IOException ex) {
            throw new RuntimeException("Serialization was failed, but it wasn't expected");
        }
    }
}

@NonNullApi
@NonNullFields
class InitResult {
    ApiServiceTransaction transaction;
    TableSchema schema;

    InitResult(ApiServiceTransaction transaction, TableSchema schema) {
        this.transaction = transaction;
        this.schema = schema;
    }
}

interface Abortable<T> {
    T abort();
}

@NonNullApi
@NonNullFields
class RetryingTableWriterBaseImpl<T> {
    static final Logger logger = LoggerFactory.getLogger(RetryingTableWriterImpl.class);

    final ApiServiceClient apiServiceClient;
    final ScheduledExecutorService executor;
    WriteTable<T> secondaryReq;
    final RpcOptions rpcOptions;
    @Nullable
    TableRowsSerializer<T> tableRowsSerializer;

    final Queue<WriteTask<T>> writeTasks = new ConcurrentLinkedQueue<>();
    final Set<Abortable<?>> processing = new HashSet<>();
    final Queue<CompletableFuture<Void>> handledEvents = new ConcurrentLinkedQueue<>();

    final Semaphore semaphore;

    final CompletableFuture<InitResult> init;
    final CompletableFuture<Void> result = new CompletableFuture<>();
    final CompletableFuture<Void> firstBufferHandled;

    volatile WriteTable<T> req;
    @Nullable
    private volatile Buffer<T> buffer;

    volatile boolean canceled = false;
    volatile boolean closed = false;
    int nextWriteTaskIndex = 0;

    volatile CompletableFuture<Void> readyEvent = CompletableFuture.completedFuture(null);

    RetryingTableWriterBaseImpl(
            ApiServiceClient apiServiceClient,
            ScheduledExecutorService executor,
            WriteTable<T> req,
            RpcOptions rpcOptions,
            SerializationResolver serializationResolver
    ) {
        req = needSetTableSchema(req) ? getRequestWithTableSchema(req) : req;

        this.apiServiceClient = apiServiceClient;
        this.executor = executor;
        this.rpcOptions = rpcOptions;

        this.req = req.toBuilder().setNeedRetries(false).build();
        this.secondaryReq = this.req.toBuilder().setPath(req.getYPath().append(true)).build();

        YPath path = this.req.getYPath();
        boolean append = path.getAppend().orElse(false);
        LockMode lockMode = append ? LockMode.Shared : LockMode.Exclusive;

        this.semaphore = new Semaphore(this.req.getMaxWritesInFlight());

        Buffer<T> firstBuffer = new Buffer<>();
        firstBufferHandled = firstBuffer.handled;
        this.buffer = firstBuffer;

        StartTransaction.Builder transactionRequestBuilder = StartTransaction.master().toBuilder();
        req.getTransactionId().ifPresent(transactionRequestBuilder::setParentId);
        this.init = apiServiceClient.startTransaction(transactionRequestBuilder.build())
                .thenCompose(transaction -> {
                    CompletableFuture<?> createNodeFuture;
                    if (!append) {
                        HashMap<String, YTreeNode> attributes = new HashMap<>();
                        this.req.getTableSchema()
                                .ifPresent(schema -> attributes.put("schema", schema.toYTree()));
                        createNodeFuture = transaction.createNode(
                                CreateNode.builder()
                                        .setPath(path)
                                        .setType(CypressNodeType.TABLE)
                                        .setAttributes(attributes)
                                        .setIgnoreExisting(true)
                                        .build());
                    } else {
                        createNodeFuture = CompletableFuture.completedFuture(transaction);
                    }
                    return createNodeFuture
                            .thenCompose(unused -> transaction.lockNode(new LockNode(path, lockMode)))
                            .thenCompose(unused -> transaction.getNode(
                                    GetNode.builder()
                                            .setPath(path.justPath())
                                            .setAttributes(List.of("schema"))
                                            .build()))
                            .thenApply(node -> new InitResult(
                                    transaction, TableSchema.fromYTree(node.getAttributeOrThrow("schema"))))
                            .thenApply(result -> {
                                this.req.getSerializationContext()
                                        .getSkiffSerializer()
                                        .ifPresent(serializer -> serializer.setTableSchema(result.schema));

                                if (this.req.getTableSchema().isEmpty()) {
                                    if (this.req.getSerializationContext().getSkiffSerializer().isPresent()) {
                                        this.req = this.req.toBuilder()
                                                .setTableSchema(result.schema)
                                                .build();
                                        this.secondaryReq = this.secondaryReq.toBuilder()
                                                .setTableSchema(result.schema)
                                                .build();
                                    }
                                } else if (!this.req.getTableSchema().get().equals(result.schema)) {
                                    throw new IllegalStateException("Incorrect table schema");
                                }

                                this.tableRowsSerializer = TableRowsSerializer.createTableRowsSerializer(
                                        this.req.getSerializationContext(), serializationResolver).orElse(null);
                                if (this.tableRowsSerializer == null) {
                                    if (this.req.getSerializationContext().getObjectClass().isEmpty()) {
                                        throw new IllegalStateException("No object clazz");
                                    }
                                    Class<T> objectClazz = this.req.getSerializationContext().getObjectClass().get();
                                    if (UnversionedRow.class.equals(objectClazz)) {
                                        this.tableRowsSerializer =
                                                (TableRowsSerializer<T>) new TableRowsWireSerializer<>(
                                                        new UnversionedRowSerializer());
                                    } else {
                                        this.tableRowsSerializer = new TableRowsWireSerializer<>(
                                                serializationResolver.createWireRowSerializer(
                                                        serializationResolver.forClass(objectClazz, result.schema)));
                                    }
                                }
                                return result;
                            });
                });

        this.init.handle((initResult, ex) -> {
            if (ex != null) {
                cancel();
            }
            return null;
        });
    }

    public TableSchema getSchema() {
        if (tableRowsSerializer == null) {
            throw new RuntimeException("No tableRowsSerializer in TableWriter");
        }
        return tableRowsSerializer.getSchema();
    }

    private boolean addAbortable(Abortable<?> abortable) {
        boolean wantAbort = false;
        synchronized (this) {
            if (canceled) {
                wantAbort = true;
            }
            processing.add(abortable);
        }
        if (wantAbort) {
            abortable.abort();
            return true;
        }
        return false;
    }

    private synchronized void removeAbortable(Abortable<?> abortable) {
        processing.remove(abortable);
    }

    private <R extends Abortable<?>, U> CompletableFuture<U> tryWith(
            CompletableFuture<R> abortableFuture,
            Function<R, CompletableFuture<U>> function
    ) {
        return abortableFuture.thenCompose(abortable -> {
            if (addAbortable(abortable)) {
                // No need to call `function`.
                return RpcUtil.failedFuture(new IllegalStateException("Already canceled"));
            }

            CompletableFuture<U> functionResult = function.apply(abortable);
            functionResult.whenComplete((res, ex) -> {
                if (ex != null) {
                    abortable.abort();
                }
                removeAbortable(abortable);
            });
            return functionResult;
        });
    }

    private boolean needSetTableSchema(WriteTable<T> req) {
        return req.getSerializationContext().getSkiffSerializer().isPresent() &&
                !req.getYPath().getAppend().orElse(false) &&
                req.getTableSchema().isEmpty();
    }

    private WriteTable<T> getRequestWithTableSchema(WriteTable<T> req) {
        return req.toBuilder()
                .setTableSchema(
                        EntityTableSchemaCreator.create(
                                req.getSerializationContext()
                                        .getObjectClass()
                                        .orElseThrow(IllegalStateException::new),
                                null
                        )
                )
                .build();
    }

    private <U> U checkedGet(CompletableFuture<U> future) {
        if (!future.isDone() && future.isCompletedExceptionally()) {
            throw new IllegalArgumentException("internal error");
        }
        return future.join();
    }

    private void processWriteTask(WriteTask<T> writeTask) {
        if (canceled) {
            // `RetryingWriter.cancel()` was called, no need to process buffers more.
            return;
        }

        writeTask.retryPolicy.onNewAttempt();

        GUID parentTxId = checkedGet(init).transaction.getId();
        CompletableFuture<ApiServiceTransaction> localTransactionFuture = apiServiceClient.startTransaction(
                StartTransaction.master().toBuilder().setParentId(parentTxId).build());

        tryWith(localTransactionFuture, localTransaction -> {
            CompletableFuture<RawTableWriter> writerFuture = localTransaction.writeTable(req)
                    .thenApply(writer -> (RawTableWriter) writer);

            return tryWith(writerFuture, writer -> writer.readyEvent()
                    .thenCompose(unused -> {
                        boolean writeResult = writer.write(writeTask.data);
                        if (!writeResult) {
                            throw new IllegalStateException("internal error");
                        }
                        return writer.finish();
                    }).thenCompose(unused -> localTransaction.commit()).thenApply(unused -> {
                        this.req = this.secondaryReq;
                        writeTask.handled.complete(null);
                        semaphore.release();

                        if (!writeTasks.isEmpty()) {
                            tryStartProcessWriteTask();
                        }
                        return null;
                    })
            );
        }).handle((unused, ex) -> {
            if (ex == null) {
                return null;
            }

            Optional<Duration> backoff = writeTask.retryPolicy.getBackoffDuration(ex, rpcOptions);
            if (backoff.isEmpty()) {
                writeTask.handled.completeExceptionally(ex);
                result.completeExceptionally(ex);
                return null;
            }

            logger.debug("Got error, we will retry it in {} seconds, message='{}'",
                    backoff.get().toNanos() / 1000000000, ex.getMessage());

            executor.schedule(
                    () -> processWriteTask(writeTask),
                    backoff.get().toNanos(),
                    TimeUnit.NANOSECONDS);

            return null;
        });
    }

    private void tryStartProcessWriteTask() {
        if (!semaphore.tryAcquire()) {
            // Max flights inflight limit was reached.
            return;
        }

        executor.execute(() -> {
            WriteTask<T> writeTask = writeTasks.peek();
            if (writeTask == null) {
                semaphore.release();
                return;
            }

            if (!firstBufferHandled.isDone() && writeTask.index > 0) {
                // This means that that first buffer is already being processed, but it isn't finished yet.
                semaphore.release();
                return;
            }

            writeTask = writeTasks.poll();
            if (writeTask == null) {
                semaphore.release();
                return;
            }

            synchronized (this) {
                if (canceled) {
                    return;
                }
                if (buffer == null && !closed) {
                    buffer = new Buffer<>();
                    readyEvent.complete(null);
                }
            }

            processWriteTask(writeTask);
        });
    }

    private void flushBuffer(boolean lastBuffer) {
        Buffer<T> currentBuffer = buffer;
        if (currentBuffer == null) {
            return;
        }

        if (lastBuffer && currentBuffer.size() == 0) {
            buffer = null;
            return;
        }

        WriteTask<T> writeTask = new WriteTask<>(
                currentBuffer, tableRowsSerializer, rpcOptions.getRetryPolicyFactory().get(), nextWriteTaskIndex++);
        writeTasks.add(writeTask);
        handledEvents.add(currentBuffer.handled);

        if (!lastBuffer && writeTasks.size() <= req.getMaxWritesInFlight()) {
            buffer = new Buffer<>();
        } else {
            buffer = null;
            readyEvent = new CompletableFuture<>();
        }

        tryStartProcessWriteTask();
    }

    public boolean write(List<T> rows, TableSchema schema) {
        if (!init.isDone() || result.isCompletedExceptionally()) {
            return false;
        }

        if (closed || canceled) {
            return false;
        }

        Buffer<T> currentBuffer = buffer;
        if (currentBuffer == null) {
            return false;
        }

        ByteBuf serializedRows = tableRowsSerializer.serializeRowsToBuf(rows, schema);
        currentBuffer.write(serializedRows);
        currentBuffer.rowsCount += rows.size();

        if (currentBuffer.size() >= req.getChunkSize()) {
            flushBuffer(false);
        }

        return true;
    }

    public CompletableFuture<Void> readyEvent() {
        if (result.isCompletedExceptionally()) {
            return result;
        }

        return CompletableFuture.anyOf(result, CompletableFuture.allOf(init, readyEvent))
                .thenCompose(unused -> CompletableFuture.completedFuture(null));
    }

    public CompletableFuture<?> close() {
        synchronized (this) {
            closed = true;
            flushBuffer(true);
        }

        return init.thenCompose(initResult -> CompletableFuture.anyOf(result, CompletableFuture.allOf(
                        handledEvents.toArray(new CompletableFuture<?>[0]))).thenApply(unused -> initResult)
                ).thenCompose(initResult -> initResult.transaction.commit())
                .whenComplete((unused, ex) -> {
                    // Exception will be saved after this stage.
                    if (ex != null) {
                        cancel();
                    }
                });
    }

    public CompletableFuture<TableSchema> getTableSchema() {
        return init.thenApply(initResult -> initResult.schema);
    }

    public synchronized void cancel() {
        canceled = true;
        buffer = null;
        readyEvent = new CompletableFuture<>();

        for (Abortable<?> abortable : processing) {
            abortable.abort();
        }

        init.join().transaction.abort();
    }
}

@NonNullApi
@NonNullFields
class RetryingTableWriterImpl<T> extends RetryingTableWriterBaseImpl<T> implements TableWriter<T> {
    RetryingTableWriterImpl(
            ApiServiceClient apiServiceClient,
            ScheduledExecutorService executor,
            WriteTable<T> req,
            RpcOptions rpcOptions,
            SerializationResolver serializationResolver
    ) {
        super(apiServiceClient, executor, req, rpcOptions, serializationResolver);
    }

    @Override
    public boolean write(List<T> rows, TableSchema schema) {
        return super.write(rows, schema);
    }

    @Override
    public CompletableFuture<Void> readyEvent() {
        return super.readyEvent();
    }

    @Override
    public CompletableFuture<?> close() {
        return super.close();
    }

    @Override
    public CompletableFuture<TableSchema> getTableSchema() {
        return super.getTableSchema();
    }

    @Override
    public TableSchema getSchema() {
        return super.getSchema();
    }

    @Override
    public synchronized void cancel() {
        super.cancel();
    }
}

@NonNullApi
@NonNullFields
class AsyncRetryingTableWriterImpl<T> extends RetryingTableWriterBaseImpl<T> implements AsyncWriter<T> {
    AsyncRetryingTableWriterImpl(
            ApiServiceClient apiServiceClient,
            ScheduledExecutorService executor,
            WriteTable<T> req,
            RpcOptions rpcOptions,
            SerializationResolver serializationResolver
    ) {
        super(apiServiceClient, executor, req, rpcOptions, serializationResolver);
    }

    @Override
    public CompletableFuture<Void> write(List<T> rows) {
        return init.thenCompose(initResult -> {
            Objects.requireNonNull(tableRowsSerializer);
            TableSchema schema;
            if (req.getTableSchema().isPresent()) {
                schema = req.getTableSchema().get();
            } else if (tableRowsSerializer.getSchema().getColumnsCount() > 0) {
                schema = tableRowsSerializer.getSchema();
            } else {
                schema = initResult.schema;
            }

            return writeImpl(rows, schema);
        });
    }

    private CompletableFuture<Void> writeImpl(List<T> rows, TableSchema schema) {
        if (write(rows, schema)) {
            return CompletableFuture.completedFuture(null);
        }
        return readyEvent().thenCompose(unused -> writeImpl(rows, schema));
    }

    @Override
    public CompletableFuture<?> finish() {
        return super.close();
    }
}
