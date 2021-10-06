package ru.yandex.yt.ytclient.proxy;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import NYT.NChunkClient.NProto.DataStatistics;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.yandex.bolts.collection.Tuple2;
import ru.yandex.inside.yt.kosher.common.GUID;
import ru.yandex.inside.yt.kosher.cypress.YPath;
import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;
import ru.yandex.yt.rpc.TResponseHeader;
import ru.yandex.yt.rpc.TStreamingFeedbackHeader;
import ru.yandex.yt.rpc.TStreamingPayloadHeader;
import ru.yandex.yt.rpcproxy.TReadFileMeta;
import ru.yandex.yt.rpcproxy.TRowsetDescriptor;
import ru.yandex.yt.rpcproxy.TRspReadFile;
import ru.yandex.yt.rpcproxy.TRspReadTable;
import ru.yandex.yt.rpcproxy.TRspReadTableMeta;
import ru.yandex.yt.rpcproxy.TRspWriteFile;
import ru.yandex.yt.rpcproxy.TRspWriteTable;
import ru.yandex.yt.rpcproxy.TWriteTableMeta;
import ru.yandex.yt.ytclient.object.WireRowSerializer;
import ru.yandex.yt.ytclient.proxy.internal.SlidingWindow;
import ru.yandex.yt.ytclient.proxy.internal.TableAttachmentReader;
import ru.yandex.yt.ytclient.proxy.request.ColumnFilter;
import ru.yandex.yt.ytclient.proxy.request.CreateNode;
import ru.yandex.yt.ytclient.proxy.request.GetNode;
import ru.yandex.yt.ytclient.proxy.request.LockMode;
import ru.yandex.yt.ytclient.proxy.request.LockNode;
import ru.yandex.yt.ytclient.proxy.request.ObjectType;
import ru.yandex.yt.ytclient.proxy.request.StartTransaction;
import ru.yandex.yt.ytclient.proxy.request.WriteTable;
import ru.yandex.yt.ytclient.rpc.RpcClient;
import ru.yandex.yt.ytclient.rpc.RpcClientResponse;
import ru.yandex.yt.ytclient.rpc.RpcClientStreamControl;
import ru.yandex.yt.ytclient.rpc.RpcOptions;
import ru.yandex.yt.ytclient.rpc.RpcStreamConsumer;
import ru.yandex.yt.ytclient.rpc.RpcUtil;
import ru.yandex.yt.ytclient.rpc.internal.Codec;
import ru.yandex.yt.ytclient.rpc.internal.Compression;
import ru.yandex.yt.ytclient.rpc.internal.LazyResponse;
import ru.yandex.yt.ytclient.tables.ColumnSchema;
import ru.yandex.yt.ytclient.tables.TableSchema;
import ru.yandex.yt.ytclient.wire.UnversionedRow;
import ru.yandex.yt.ytclient.wire.UnversionedValue;
import ru.yandex.yt.ytclient.wire.WireProtocolWriter;

abstract class StreamBase<RspType extends Message> implements RpcStreamConsumer {
    protected static final Logger logger = LoggerFactory.getLogger(StreamBase.class);

    protected final CompletableFuture<RpcClientResponse<RspType>> result = new CompletableFuture<>();

    private final CompletableFuture<RpcClientStreamControl> controlFuture = new CompletableFuture<>();

    protected volatile RpcClientStreamControl control;

    protected Compression compression;
    protected Codec codec = null;
    private int currentCodecId = -1;

    @Override
    public void onStartStream(RpcClientStreamControl control) {
        this.control = control;
        controlFuture.complete(control);
    }

    protected abstract Parser<RspType> responseParser();

    protected void maybeReinitCodec(int codecId) {
        if (currentCodecId != codecId) {
            compression = Compression.fromValue(codecId);
            codec = Codec.codecFor(compression);
            currentCodecId = codecId;
        }
    }

    List<byte[]> decompressedAttachments(int codecId, List<byte[]> attachments) {
        maybeReinitCodec(codecId);

        List<byte[]> decompressed;
        if (currentCodecId == 0) {
            decompressed = attachments;
        } else {
            decompressed = new ArrayList<>();
            for (byte[] attachment : attachments) {
                if (attachment == null) {
                    decompressed.add(null);
                } else {
                    decompressed.add(codec.decompress(attachment));
                }
            }
        }

        return decompressed;
    }

    @Override
    public void onResponse(RpcClient sender, TResponseHeader header, List<byte[]> attachments) {
        if (!result.isDone()) {
            if (attachments.size() < 1 || attachments.get(0) == null) {
                throw new IllegalStateException("Received response without a body");
            }

            attachments = decompressedAttachments(header.getCodec(), attachments);

            result.complete(
                    new LazyResponse<>(
                            responseParser(),
                            attachments.get(0),
                            new ArrayList<>(attachments.subList(1, attachments.size())), sender,
                            header));
        }
    }

    @Override
    public void onError(Throwable error) {
        logger.error("Error", error);

        result.completeExceptionally(error);
    }

    @Override
    public void onCancel(CancellationException cancel) {
        result.completeExceptionally(cancel);
    }

    public void cancel() {
        controlFuture.thenApply(RpcClientStreamControl::cancel);
    }

    /**
     * Useful in tests. Get rpc proxy address this writer is connected to.
     */
    @Nullable
    String getRpcProxyAddress() {
        return control.getRpcProxyAddress();
    }
}

//
// Writer stuff
//

interface DataSupplier {
    byte[] get();

    default int put(byte[] data) {
        throw new IllegalArgumentException();
    }

    default boolean hasData() {
        return true;
    }
}

class MessagesSupplier implements DataSupplier {
    private final LinkedList<byte[]> messages = new LinkedList<>();

    @Override
    public byte[] get() {
        return messages.removeFirst();
    }

    @Override
    public boolean hasData() {
        return !messages.isEmpty();
    }

    @Override
    public int put(byte[] data) {
        messages.add(data);
        return RpcUtil.attachmentSize(data);
    }
}

class WrappedSupplier implements DataSupplier {
    private final DataSupplier supplier;
    private final Codec inputCodec;
    private boolean eof;

    WrappedSupplier(DataSupplier supplier, Codec inputCodec) {
        this.supplier = supplier;
        this.inputCodec = inputCodec;
    }

    @Override
    public byte[] get() {
        byte[] data = supplier.get();
        eof = data == null;
        return data;
    }

    @Override
    public int put(byte[] data) {
        if (eof) {
            throw new IllegalArgumentException();
        }

        if (data == null) {
            return supplier.put(null);
        } else {
            return supplier.put(inputCodec.compress(data));
        }
    }

    @Override
    public boolean hasData() {
        return !eof && supplier.hasData();
    }
}

abstract class StreamWriterImpl<T extends Message> extends StreamBase<T> implements RpcStreamConsumer, StreamWriter {
    private static final CompletableFuture<Void> COMPLETED_FUTURE = CompletableFuture.completedFuture(null);

    protected final CompletableFuture<List<byte[]>> startUpload = new CompletableFuture<>();

    private final Object lock = new Object();
    private volatile DataSupplier supplier;

    private CompletableFuture<Void> readyEvent = new CompletableFuture<>();
    private long writePosition = 0;
    private long readPosition = 0;

    private final long windowSize;
    private final long packetSize;

    private final List<byte[]> payloadAttachments = new LinkedList<>();
    private long payloadOffset = 0;

    private final AtomicBoolean closed = new AtomicBoolean(false);

    protected StreamWriterImpl(long windowSize, long packetSize) {
        this.windowSize = windowSize;
        this.packetSize = packetSize;

        result.whenComplete((unused, ex) -> {
            if (ex != null) {
                startUpload.completeExceptionally(ex);
            }
        });
    }

    @Override
    public void onStartStream(RpcClientStreamControl control) {
        this.supplier = new WrappedSupplier(
                new MessagesSupplier(),
                Codec.codecFor(control.getExpectedPayloadCompression())
        );
        super.onStartStream(control);
    }

    private void reinitReadyEvent() {
        this.readyEvent.complete(null);
        this.readyEvent = new CompletableFuture<>();
    }

    private void uploadSome() {
        synchronized (lock) {
            if (!supplier.hasData()) {
                return;
            }
        }

        final LinkedList<byte[]> readyToUpload = new LinkedList<>();

        long sendSize = 0;

        synchronized (lock) {
            while (supplier.hasData() && sendSize < windowSize) {
                byte[] next = supplier.get();

                readyToUpload.add(next);
                sendSize += RpcUtil.attachmentSize(next);
            }
        }

        while (!readyToUpload.isEmpty()) {
            final List<byte[]> packet = new ArrayList<>();
            long currentPacketSize = 0;

            while (!readyToUpload.isEmpty() && currentPacketSize < packetSize) {
                byte[] data = readyToUpload.peekFirst();
                packet.add(data);
                currentPacketSize += RpcUtil.attachmentSize(data);
                readyToUpload.removeFirst();
            }

            if (logger.isTraceEnabled()) {
                StringBuilder stringBuilder = new StringBuilder();
                stringBuilder.append("[");
                for (byte[] data : packet) {
                    stringBuilder.append(RpcUtil.attachmentSize(data));
                    stringBuilder.append(", ");
                }
                stringBuilder.append("]");

                logger.trace("Packet: {} {}", stringBuilder.toString(), writePosition - readPosition);
            }

            control.sendPayload(packet);
        }
    }

    @Override
    public void onFeedback(RpcClient sender, TStreamingFeedbackHeader header, List<byte[]> attachments) {
        if (!attachments.isEmpty()) {
            throw new IllegalArgumentException("protocol error in onFeedback");
        }

        synchronized (lock) {
            readPosition = header.getReadPosition();
            if (writePosition - readPosition < windowSize) {
                reinitReadyEvent();
            }
        }

        uploadSome();
    }

    @Override
    public void onPayload(RpcClient sender, TStreamingPayloadHeader header, List<byte[]> attachments) {
        boolean eof = false;

        maybeReinitCodec(header.getCodec());

        for (byte[] attachment : attachments) {
            payloadOffset += RpcUtil.attachmentSize(attachment);
            if (attachment != null) {
                payloadAttachments.add(codec.decompress(attachment));
            } else {
                eof = true;
            }
        }

        if (eof) {
            if (!startUpload.isDone()) {
                startUpload.complete(payloadAttachments);
            } else {
                throw new IllegalArgumentException("protocol error in onPayload");
            }
        }

        control.feedback(payloadOffset);
    }

    @Override
    public void onWakeup() {
        uploadSome();
    }

    protected boolean push(byte[] data) {
        if (result.isCompletedExceptionally()) {
            result.join();
        }

        synchronized (lock) {
            if (writePosition - readPosition >= windowSize) {
                return false;
            }

            writePosition += supplier.put(data);

            if (writePosition - readPosition < windowSize) {
                reinitReadyEvent();
            }
        }

        control.wakeUp();

        if (closed.get() && data != null) {
            throw new IllegalStateException("StreamWriter is already closed");
        }

        return true;
    }

    @Override
    public void onError(Throwable error) {
        super.onError(error);

        synchronized (lock) {
            reinitReadyEvent();
        }
    }

    @Override
    public void onCancel(CancellationException cancel) {
        super.onCancel(cancel);

        synchronized (lock) {
            reinitReadyEvent();
        }
    }

    @Override
    public CompletableFuture<Void> readyEvent() {
        synchronized (lock) {
            if (writePosition - readPosition < windowSize) {
                return COMPLETED_FUTURE;
            } else {
                return this.readyEvent;
            }
        }
    }

    @Override
    public CompletableFuture<?> close() {
        closed.set(true);
        return readyEvent()
                .thenAccept((unused) -> push(null))
                .thenCompose((unused) -> result);
    }
}


@NonNullApi
interface RawTableWriter extends Abortable<Void> {
    boolean write(byte[] attachment);

    // Finishes a table writing.
    CompletableFuture<?> finish();

    // Aborts a table writing.
    Void abort();

    // Wait it before trying to write again.
    CompletableFuture<Void> readyEvent();
}

@NonNullApi
class RawTableWriterImpl extends StreamWriterImpl<TRspWriteTable>
        implements RawTableWriter, RpcStreamConsumer {
    RawTableWriterImpl(long windowSize, long packetSize) {
        super(windowSize, packetSize);
    }

    @Override
    protected Parser<TRspWriteTable> responseParser() {
        return TRspWriteTable.parser();
    }

    @Override
    public boolean write(byte[] attachment) {
        return push(attachment);
    }

    @Override
    public CompletableFuture<?> finish() {
        return close();
    }

    @Override
    public Void abort() {
        cancel();
        return null;
    }

    @Override
    public CompletableFuture<Void> readyEvent() {
        return super.readyEvent();
    }
}

@NonNullApi
@NonNullFields
class TableRowsSerializer<T> {
    private TRowsetDescriptor rowsetDescriptor = TRowsetDescriptor.newBuilder().build();
    private final WireRowSerializer<T> rowSerializer;
    private final Map<String, Integer> column2id = new HashMap<>();

    TableRowsSerializer(WireRowSerializer<T> rowSerializer) {
        this.rowSerializer = Objects.requireNonNull(rowSerializer);
    }

    public WireRowSerializer<T> getRowSerializer() {
        return rowSerializer;
    }

    public TRowsetDescriptor getRowsetDescriptor() {
        return rowsetDescriptor;
    }

    public ByteBuf serializeRows(List<T> rows, TableSchema schema) {
        TRowsetDescriptor currentDescriptor = getCurrentRowsetDescriptor(schema);
        int[] idMapping = getIdMapping(rows, schema);

        ByteBuf buf = Unpooled.buffer();
        writeMergedRowWithoutCount(buf, currentDescriptor, rows, idMapping);

        updateRowsetDescriptor(currentDescriptor);

        return buf;
    }

    public byte[] serialize(ByteBuf serializedRows, int rowsCount) throws IOException {
        ByteBuf buf = Unpooled.buffer();

        // parts
        buf.writeIntLE(2);

        int descriptorSizeIndex = buf.writerIndex();
        buf.writeLongLE(0); // reserve space

        writeDescriptor(buf, rowsetDescriptor);

        buf.setLongLE(descriptorSizeIndex, buf.writerIndex() - descriptorSizeIndex - 8);

        int mergedRowSizeIndex = buf.writerIndex();
        buf.writeLongLE(0); // reserve space

        writeMergedRow(buf, serializedRows, rowsCount);

        buf.setLongLE(mergedRowSizeIndex, buf.writerIndex() - mergedRowSizeIndex - 8);

        return bufToArray(buf);
    }

    public byte[] serialize(List<T> rows, TableSchema schema) throws IOException {
        TRowsetDescriptor currentDescriptor = getCurrentRowsetDescriptor(schema);
        int[] idMapping = getIdMapping(rows, schema);

        ByteBuf buf = Unpooled.buffer();
        writeRowsData(buf, currentDescriptor, rows, idMapping);

        updateRowsetDescriptor(currentDescriptor);

        return bufToArray(buf);
    }

    private TRowsetDescriptor getCurrentRowsetDescriptor(TableSchema schema) {
        TRowsetDescriptor.Builder builder = TRowsetDescriptor.newBuilder();

        for (ColumnSchema descriptor : schema.getColumns()) {
            if (!column2id.containsKey(descriptor.getName())) {
                builder.addNameTableEntries(TRowsetDescriptor.TNameTableEntry.newBuilder()
                        .setName(descriptor.getName())
                        .setType(descriptor.getType().getValue())
                        .build());

                column2id.put(descriptor.getName(), column2id.size());
            }
        }

        return builder.build();
    }

    private int[] getIdMapping(List<T> rows, TableSchema schema) {
        Iterator<T> it = rows.iterator();
        if (!it.hasNext()) {
            throw new IllegalStateException();
        }

        T first = it.next();
        boolean isUnversionedRows = first instanceof List && ((List<?>) first).get(0) instanceof UnversionedRow;

        int[] idMapping = isUnversionedRows
                ? new int[column2id.size()]
                : null;

        if (isUnversionedRows) {
            for (UnversionedRow row : (List<UnversionedRow>) rows) {
                List<UnversionedValue> values = row.getValues();
                for (int columnNumber = 0;
                     columnNumber < schema.getColumns().size() && columnNumber < values.size();
                     ++columnNumber
                ) {
                    String columnName = schema.getColumnName(columnNumber);
                    UnversionedValue value = values.get(columnNumber);
                    int columnId = column2id.get(columnName);
                    idMapping[value.getId()] = columnId;
                }
            }
        }

        return idMapping;
    }

    private void updateRowsetDescriptor(TRowsetDescriptor currentDescriptor) {
        if (currentDescriptor.getNameTableEntriesCount() > 0) {
            TRowsetDescriptor.Builder merged = TRowsetDescriptor.newBuilder();
            merged.mergeFrom(rowsetDescriptor);
            merged.addAllNameTableEntries(currentDescriptor.getNameTableEntriesList());
            rowsetDescriptor = merged.build();
        }
    }

    private byte[] bufToArray(ByteBuf buf) {
        buf.array();
        byte[] attachment = new byte[buf.readableBytes()];
        buf.readBytes(attachment, 0, attachment.length);

        if (buf.readableBytes() != 0) {
            throw new IllegalStateException();
        }

        return attachment;
    }

    private void writeDescriptor(ByteBuf buf, TRowsetDescriptor descriptor) throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        CodedOutputStream os = CodedOutputStream.newInstance(byteArrayOutputStream);
        descriptor.writeTo(os);
        os.flush();

        buf.writeBytes(byteArrayOutputStream.toByteArray());
    }

    private void writeMergedRow(ByteBuf buf, TRowsetDescriptor descriptor, List<T> rows, int[] idMapping) {
        WireProtocolWriter writer = new WireProtocolWriter();
        rowSerializer.updateSchema(descriptor);
        writer.writeUnversionedRowset(rows, rowSerializer, idMapping);

        for (byte[] bytes : writer.finish()) {
            buf.writeBytes(bytes);
        }
    }

    private void writeMergedRowWithoutCount(ByteBuf buf, TRowsetDescriptor descriptor, List<T> rows, int[] idMapping) {
        WireProtocolWriter writer = new WireProtocolWriter();
        rowSerializer.updateSchema(descriptor);
        writer.writeUnversionedRowsetWithoutCount(rows, rowSerializer, idMapping);

        for (byte[] bytes : writer.finish()) {
            buf.writeBytes(bytes);
        }
    }

    private void writeMergedRow(ByteBuf buf, ByteBuf serializedRows, int rowsCount) {
        WireProtocolWriter writer = new WireProtocolWriter();
        writer.writeUnversionedRowset(serializedRows, rowsCount);

        for (byte[] bytes : writer.finish()) {
            buf.writeBytes(bytes);
        }
    }

    private void writeRowsData(
            ByteBuf buf,
            TRowsetDescriptor descriptorDelta,
            List<T> rows,
            int[] idMapping
    ) throws IOException {
        // parts
        buf.writeIntLE(2);

        int descriptorDeltaSizeIndex = buf.writerIndex();
        buf.writeLongLE(0);  // reserve space

        writeDescriptor(buf, descriptorDelta);

        buf.setLongLE(descriptorDeltaSizeIndex, buf.writerIndex() - descriptorDeltaSizeIndex - 8);

        int mergedRowSizeIndex = buf.writerIndex();
        buf.writeLongLE(0);  // reserve space

        writeMergedRow(buf, descriptorDelta, rows, idMapping);

        buf.setLongLE(mergedRowSizeIndex, buf.writerIndex() - mergedRowSizeIndex - 8);
    }
}

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
        this.buffer =  Unpooled.buffer();
    }

    public int size() {
        return buffer.readableBytes();
    }

    public void write(ByteBuf buf) {
        buffer.writeBytes(buf);
    }

    public byte[] finish(TableRowsSerializer<T> rowsSerializer) {
        try {
            return rowsSerializer.serialize(buffer, rowsCount);
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
class RetryingTableWriterImpl<T> implements TableWriter<T> {
    static final Logger logger = LoggerFactory.getLogger(RetryingTableWriterImpl.class);

    final ApiServiceClient apiServiceClient;
    final ScheduledExecutorService executor;
    final WriteTable<T> secondaryReq;
    final RpcOptions rpcOptions;
    final TableRowsSerializer<T> rowsSerializer;

    final Queue<WriteTask<T>> writeTasks = new ConcurrentLinkedQueue<>();
    final Set<Abortable<?>> processing = new HashSet<>();
    final Queue<CompletableFuture<Void>> handledEvents = new ConcurrentLinkedQueue<>();

    final Semaphore semaphore;

    final CompletableFuture<InitResult> init;
    final CompletableFuture<Void> result = new CompletableFuture<>();
    final CompletableFuture<Void> firstBufferHandled;

    volatile WriteTable<T> req;
    @Nullable private volatile Buffer<T> buffer;

    volatile boolean canceled = false;
    volatile boolean closed = false;
    int nextWriteTaskIndex = 0;

    volatile CompletableFuture<Void> readyEvent = CompletableFuture.completedFuture(null);

    RetryingTableWriterImpl(
            ApiServiceClient apiServiceClient,
            ScheduledExecutorService executor,
            WriteTable<T> req,
            RpcOptions rpcOptions
    ) {
        Objects.requireNonNull(req.getYPath());

        this.apiServiceClient = apiServiceClient;
        this.executor = executor;
        this.rpcOptions = rpcOptions;

        this.req = new WriteTable<>(req);
        this.req.setNeedRetries(false);
        this.secondaryReq = new WriteTable<>(this.req);
        this.secondaryReq.setPath(this.secondaryReq.getYPath().append(true));

        this.rowsSerializer = new TableRowsSerializer<>(this.req.getSerializer());

        YPath path = this.req.getYPath();
        boolean append = path.getAppend().orElse(false);
        LockMode lockMode = append ? LockMode.Shared : LockMode.Exclusive;

        this.semaphore = new Semaphore(this.req.getMaxWritesInFlight());

        Buffer<T> firstBuffer = new Buffer<>();
        firstBufferHandled = firstBuffer.handled;
        this.buffer = firstBuffer;

        this.init = apiServiceClient.startTransaction(StartTransaction.master())
                .thenCompose(transaction -> {
                    CompletableFuture<?> createNodeFuture;
                    if (!append) {
                        createNodeFuture = transaction.createNode(
                                new CreateNode(path, ObjectType.Table).setIgnoreExisting(true));
                    } else {
                        createNodeFuture = CompletableFuture.completedFuture(transaction);
                    }
                    return createNodeFuture
                            .thenCompose(unused -> transaction.lockNode(new LockNode(path, lockMode)))
                            .thenCompose(unused -> transaction.getNode(
                                    new GetNode(path.justPath()).setAttributes(ColumnFilter.of("schema"))))
                            .thenApply(node -> new InitResult(
                                    transaction, TableSchema.fromYTree(node.getAttributeOrThrow("schema"))));
                });

        this.init.handle((initResult, ex) -> {
            if (ex != null) {
                cancel();
            }
            return null;
        });
    }

    @Override
    public WireRowSerializer<T> getRowSerializer() {
        return rowsSerializer.getRowSerializer();
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
                StartTransaction.master().setParentId(parentTxId));

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
            if (!backoff.isPresent()) {
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
                currentBuffer, rowsSerializer, rpcOptions.getRetryPolicyFactory().get(), nextWriteTaskIndex++);
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

    @Override
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

        ByteBuf serializedRows = rowsSerializer.serializeRows(rows, schema);
        currentBuffer.write(serializedRows);
        currentBuffer.rowsCount += rows.size();

        if (currentBuffer.size() >= req.getChunkSize()) {
            flushBuffer(false);
        }

        return true;
    }

    @Override
    public CompletableFuture<Void> readyEvent() {
        if (result.isCompletedExceptionally()) {
            return result;
        }

        return CompletableFuture.anyOf(result, CompletableFuture.allOf(init, readyEvent))
                .thenCompose(unused -> CompletableFuture.completedFuture(null));
    }

    @Override
    public CompletableFuture<?> close() {
        synchronized (this) {
            closed = true;
            flushBuffer(true);
        }

        return init.thenCompose(initResult -> CompletableFuture.anyOf(result, CompletableFuture.allOf(
                handledEvents.toArray(new CompletableFuture<?>[0]))).thenApply(unused -> initResult)
        ).thenCompose(initResult -> initResult.transaction.commit()
        ).whenComplete((unused, ex) -> {
            // Exception will be saved after this stage.
            if (ex != null) {
                cancel();
            }
        });
    }

    @Override
    public TRowsetDescriptor getRowsetDescriptor() {
        return rowsSerializer.getRowsetDescriptor();
    }

    @Override
    public CompletableFuture<TableSchema> getTableSchema() {
        return init.thenApply(initResult -> initResult.schema);
    }

    @Override
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
class TableWriterImpl<T> extends RawTableWriterImpl implements TableWriter<T>, RpcStreamConsumer {
    @Nullable private TableSchema schema;
    @Nonnull final TableRowsSerializer<T> rowsSerializer;

    TableWriterImpl(long windowSize, long packetSize, WireRowSerializer<T> serializer) {
        super(windowSize, packetSize);
        this.rowsSerializer = new TableRowsSerializer<>(serializer);
    }

    public WireRowSerializer<T> getRowSerializer() {
        return rowsSerializer.getRowSerializer();
    }

    public CompletableFuture<TableWriter<T>> startUpload() {
        TableWriterImpl<T> self = this;

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

            return self;
        });
    }

    @Override
    public boolean write(List<T> rows, TableSchema schema) throws IOException {
        byte[] serializedRows = rowsSerializer.serialize(rows, schema);
        return write(serializedRows);
    }

    @Override
    public TRowsetDescriptor getRowsetDescriptor() {
        return rowsSerializer.getRowsetDescriptor();
    }

    @Override
    public CompletableFuture<TableSchema> getTableSchema() {
        return CompletableFuture.completedFuture(schema);
    }
}

class FileWriterImpl extends StreamWriterImpl<TRspWriteFile> implements FileWriter, RpcStreamConsumer {
    FileWriterImpl(long windowSize, long packetSize) {
        super(windowSize, packetSize);
    }

    @Override
    protected Parser<TRspWriteFile> responseParser() {
        return TRspWriteFile.parser();
    }

    public CompletableFuture<FileWriter> startUpload() {
        return startUpload.thenApply((unused) -> this);
    }

    @Override
    public boolean write(byte[] data, int offset, int len) {
        if (data != null) {
            byte[] newData = new byte[len - offset];
            System.arraycopy(data, offset, newData, 0, len);
            data = newData;
        }

        return push(data);
    }
}

//
// Reader stuff
//

class Stash {
    protected static final Logger logger = LoggerFactory.getLogger(StreamReaderImpl.class);

    private final CompletableFuture<Void> completedFuture = CompletableFuture.completedFuture(null);
    private CompletableFuture<Void> readyEvent = new CompletableFuture<>();
    private Throwable ex = null;
    private boolean eof = false;
    private long offset = 0;

    private final LinkedList<Tuple2<byte[], Long>> attachments = new LinkedList<>();

    void push(Attachment attachment) throws Throwable {
        synchronized (attachments) {
            if (ex != null) {
                throw ex;
            }

            boolean needWakeup = attachments.isEmpty() && !eof;
            offset += attachment.getCompressedSize();

            attachments.addLast(new Tuple2<>(attachment.getDecompressedBytes(), offset));

            if (needWakeup) {
                this.readyEvent.complete(null);
                readyEvent = new CompletableFuture<>();
            }
        }
    }

    boolean isEof() {
        synchronized (attachments) {
            return eof;
        }
    }

    byte[] pop(RpcClientStreamControl control) {
        synchronized (attachments) {
            if (attachments.isEmpty()) {
                return null;
            } else {
                Tuple2<byte[], Long> message = attachments.removeFirst();
                control.feedback(message._2);
                eof = message._1 == null;
                return message._1;
            }
        }
    }

    CompletableFuture<Void> readyEvent() {
        synchronized (attachments) {
            if (attachments.isEmpty() && !eof) {
                return this.readyEvent;
            } else {
                return completedFuture;
            }
        }
    }

    void error(Throwable ex) {
        synchronized (attachments) {
            this.ex = ex;
            this.readyEvent.completeExceptionally(ex);
        }
    }
}

class Attachment {
    private final long compressedSize;
    private final byte[] decompressedBytes;

    Attachment(long compressedSize, byte[] decompressedBytes) {
        this.compressedSize = compressedSize;
        this.decompressedBytes = decompressedBytes;
    }

    public long getCompressedSize() {
        return compressedSize;
    }

    public byte[] getDecompressedBytes() {
        return decompressedBytes;
    }
}

class Payload {
    private final List<Attachment> attachments;
    private final RpcClient sender;

    Payload(List<Attachment> attachments, RpcClient sender) {
        this.attachments = attachments;
        this.sender = sender;
    }

    public List<Attachment> getAttachments() {
        return attachments;
    }

    public RpcClient getSender() {
        return sender;
    }
}

abstract class StreamReaderImpl<RspType extends Message> extends StreamBase<RspType> {
    private static final int MAX_WINDOW_SIZE = 16384;

    private final Stash stash = new Stash();
    private final SlidingWindow<Payload> window = new SlidingWindow<>(MAX_WINDOW_SIZE, payload -> {
        for (Attachment attachment : payload.getAttachments()) {
            try {
                stash.push(attachment);
            } catch (Throwable ex) {
                onError(ex);
            }
        }
    });

    StreamReaderImpl() {
        result.whenComplete((unused, ex) -> {
            if (ex != null) {
                stash.error(ex);
            }
        });
    }

    @Override
    public void onStartStream(RpcClientStreamControl control) {
        super.onStartStream(control);
        control.sendEof();
    }

    @Override
    public void onFeedback(RpcClient sender, TStreamingFeedbackHeader header, List<byte[]> attachments) {
    }

    @Override
    public void onPayload(RpcClient sender, TStreamingPayloadHeader header, List<byte[]> attachments) {
        if (attachments.isEmpty()) {
            throw new IllegalArgumentException("Empty attachments");
        }

        int sequenceNumber = header.getSequenceNumber();
        maybeReinitCodec(header.getCodec());

        List<Attachment> attachmentList = new ArrayList<>(attachments.size());
        for (byte[] attachment : attachments) {
            long size = attachment == null
                    ? 1
                    : attachment.length;

            byte[] attachmentDecompressed = attachment != null
                    ? codec.decompress(attachment)
                    : null;

            attachmentList.add(new Attachment(size, attachmentDecompressed));
        }
        window.add(sequenceNumber, new Payload(attachmentList, sender));
    }

    CompletableFuture<byte[]> readHead() {
        return getReadyEvent().thenApply((unused) -> stash.pop(control));
    }

    boolean doCanRead() {
        return !stash.isEof();
    }

    byte[] doRead() throws Exception {
        if (result.isCompletedExceptionally()) {
            result.get();
            return null;
        }

        return stash.pop(control);
    }

    CompletableFuture<Void> getReadyEvent() {
        return CompletableFuture.anyOf(stash.readyEvent(), result).thenAccept((unused) -> {
        });
    }

    CompletableFuture<Void> doClose() {
        control.cancel();
        return result.handle((unused, error) -> null);
    }

    @Override
    public void onWakeup() {
    }
}

class TableReaderImpl<T> extends StreamReaderImpl<TRspReadTable> implements TableReader<T> {
    private static final Parser<TRspReadTableMeta> META_PARSER = TRspReadTableMeta.parser();

    private final TableAttachmentReader<T> reader;
    private TRspReadTableMeta metadata = null;

    TableReaderImpl(TableAttachmentReader<T> reader) {
        this.reader = reader;
    }

    @Override
    protected Parser<TRspReadTable> responseParser() {
        return TRspReadTable.parser();
    }

    @Override
    public long getStartRowIndex() {
        return metadata.getStartRowIndex();
    }

    @Override
    public long getTotalRowCount() {
        return reader.getTotalRowCount();
    }

    @Override
    public DataStatistics.TDataStatistics getDataStatistics() {
        return reader.getDataStatistics();
    }

    @Override
    public TableSchema getTableSchema() {
        return ApiServiceUtil.deserializeTableSchema(metadata.getSchema());
    }

    @Override
    public TableSchema getCurrentReadSchema() {
        final TableSchema schema = reader.getCurrentReadSchema();
        return schema != null ? schema : getTableSchema();
    }

    @Override
    public List<String> getOmittedInaccessibleColumns() {
        return metadata.getOmittedInaccessibleColumnsList();
    }

    @Override
    public TRowsetDescriptor getRowsetDescriptor() {
        return reader.getRowsetDescriptor();
    }

    public CompletableFuture<TableReader<T>> waitMetadata() {
        TableReaderImpl<T> self = this;
        return readHead().thenApply((data) -> {
            self.metadata = RpcUtil.parseMessageBodyWithCompression(data, META_PARSER, Compression.None);
            return self;
        });
    }

    @Override
    public boolean canRead() {
        return doCanRead();
    }

    @Override
    public List<T> read() throws Exception {
        return reader.parse(doRead());
    }

    @Override
    public CompletableFuture<Void> close() {
        return doClose();
    }

    @Override
    public CompletableFuture<Void> readyEvent() {
        return getReadyEvent();
    }
}

class FileReaderImpl extends StreamReaderImpl<TRspReadFile> implements FileReader {
    private long revision = -1;

    FileReaderImpl() {
    }

    @Override
    protected Parser<TRspReadFile> responseParser() {
        return TRspReadFile.parser();
    }

    @Override
    public long revision() {
        return this.revision;
    }

    public CompletableFuture<FileReader> waitMetadata() {
        FileReaderImpl self = this;
        return readHead().thenApply((data) -> {
            TReadFileMeta meta = RpcUtil.parseMessageBodyWithCompression(
                    data,
                    TReadFileMeta.parser(),
                    Compression.None
            );
            self.revision = meta.getRevision();
            return self;
        });
    }

    @Override
    public boolean canRead() {
        return doCanRead();
    }

    @Override
    public byte[] read() throws Exception {
        return doRead();
    }

    @Override
    public CompletableFuture<Void> close() {
        return doClose();
    }

    @Override
    public CompletableFuture<Void> readyEvent() {
        return getReadyEvent();
    }
}
