package ru.yandex.yt.ytclient.proxy;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;

import javax.annotation.Nullable;

import NYT.NChunkClient.NProto.DataStatistics;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.yandex.bolts.collection.Option;
import ru.yandex.bolts.collection.Tuple2;
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
import ru.yandex.yt.ytclient.rpc.RpcClient;
import ru.yandex.yt.ytclient.rpc.RpcClientResponse;
import ru.yandex.yt.ytclient.rpc.RpcClientStreamControl;
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

    volatile protected RpcClientStreamControl control;

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
                            Option.of(header)));
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

interface DataSupplier
{
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
    private final static CompletableFuture<Void> completedFuture = CompletableFuture.completedFuture(null);

    final protected CompletableFuture<List<byte[]>> startUpload = new CompletableFuture<>();

    private final Object lock = new Object();
    private volatile DataSupplier supplier;

    private CompletableFuture<Void> readyEvent = new CompletableFuture<>();
    private long writePosition = 0;
    private long readPosition = 0;

    private final long windowSize;
    private final long packetSize;

    private final List<byte[]> payloadAttachments = new LinkedList<>();
    private long payloadOffset = 0;

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
        this.supplier = new WrappedSupplier(new MessagesSupplier(), Codec.codecFor(control.getExpectedPayloadCompression()));
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
            throw new IllegalArgumentException("protocol error");
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
                throw new IllegalArgumentException("protocol error");
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
                return completedFuture;
            } else {
                return this.readyEvent;
            }
        }
    }

    @Override
    public CompletableFuture<?> close() {
        push(null);

        return result;
    }
}

class TableWriterImpl<T> extends StreamWriterImpl<TRspWriteTable> implements TableWriter<T>, RpcStreamConsumer {
    private TableSchema schema;
    private TRowsetDescriptor rowsetDescriptor = TRowsetDescriptor.newBuilder().build();
    private final WireRowSerializer<T> serializer;
    private final Map<String, Integer> column2id = new HashMap<>();

    public TableWriterImpl(long windowSize, long packetSize, WireRowSerializer<T> serializer) {
        super(windowSize, packetSize);

        this.serializer = Objects.requireNonNull(serializer);
    }

    public WireRowSerializer<T> getRowSerializer() {
        return this.serializer;
    }

    @Override
    protected Parser<TRspWriteTable> responseParser() {
        return TRspWriteTable.parser();
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

            TWriteTableMeta metadata = RpcUtil.parseMessageBodyWithCompression(head, TWriteTableMeta.parser(), Compression.None);
            self.schema = ApiServiceUtil.deserializeTableSchema(metadata.getSchema());

            logger.debug("schema -> {}", schema.toYTree().toString());

            return self;
        });
    }

    private void writeDescriptorDelta(ByteBuf buf, TRowsetDescriptor descriptor) throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        CodedOutputStream os = CodedOutputStream.newInstance(byteArrayOutputStream);
        descriptor.writeTo(os);
        os.flush();

        buf.writeBytes(byteArrayOutputStream.toByteArray());
    }

    private void writeMergedRow(ByteBuf buf, TRowsetDescriptor descriptor, List<T> rows, int[] idMapping) {
        WireProtocolWriter writer = new WireProtocolWriter();
        serializer.updateSchema(descriptor);
        writer.writeUnversionedRowset(rows, serializer, idMapping);

        for (byte [] bytes : writer.finish()) {
            buf.writeBytes(bytes);
        }
    }

    private void writeRowsData(ByteBuf buf, TRowsetDescriptor descriptor, List<T> rows, int[] idMapping) throws IOException {
        // parts
        buf.writeIntLE(2);

        int descriptorDeltaSizeIndex = buf.writerIndex();
        buf.writeLongLE(0); // reserve space

        writeDescriptorDelta(buf, descriptor);

        buf.setLongLE(descriptorDeltaSizeIndex, buf.writerIndex() - descriptorDeltaSizeIndex - 8);

        int mergedRowSizeIndex = buf.writerIndex();
        buf.writeLongLE(0); // reserve space

        writeMergedRow(buf, descriptor, rows, idMapping);

        buf.setLongLE(mergedRowSizeIndex, buf.writerIndex() - mergedRowSizeIndex - 8);
    }

    @Override
    public boolean write(List<T> rows, TableSchema schema) throws IOException {
        Iterator<T> it = rows.iterator();
        if (!it.hasNext()) {
            throw new IllegalStateException();
        }

        T first = it.next();
        boolean isUnversionedRows = first instanceof List && ((List<?>) first).get(0) instanceof UnversionedRow;

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

        ByteBuf buf = Unpooled.buffer();

        TRowsetDescriptor currentDescriptor = builder.build();

        int[] idMapping = isUnversionedRows
            ? new int[column2id.size()]
            : null;

        if (isUnversionedRows) {
            for (UnversionedRow row : (List<UnversionedRow>)rows) {
                List<UnversionedValue> values = row.getValues();
                for (int columnNumber = 0; columnNumber < schema.getColumns().size() && columnNumber < values.size(); ++columnNumber) {
                    String columnName = schema.getColumnName(columnNumber);
                    UnversionedValue value = values.get(columnNumber);
                    int columnId = column2id.get(columnName);
                    idMapping[value.getId()] = columnId;
                }
            }
        }

        writeRowsData(buf, currentDescriptor, rows, idMapping);

        byte[] attachment = new byte[buf.readableBytes()];
        buf.readBytes(attachment, 0, attachment.length);

        if (buf.readableBytes() != 0) {
            throw new IllegalStateException();
        }

        if (currentDescriptor.getNameTableEntriesCount() > 0) {
            TRowsetDescriptor.Builder merged = TRowsetDescriptor.newBuilder();
            merged.mergeFrom(rowsetDescriptor);
            merged.addAllNameTableEntries(currentDescriptor.getNameTableEntriesList());
            rowsetDescriptor = merged.build();
        }

        return push(attachment);
    }

    @Override
    public TRowsetDescriptor getRowsetDescriptor() {
        return rowsetDescriptor;
    }

    @Override
    public TableSchema getTableSchema() {
        return schema;
    }
}

class FileWriterImpl extends StreamWriterImpl<TRspWriteFile> implements FileWriter, RpcStreamConsumer {
    public FileWriterImpl(long windowSize, long packetSize) {
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
            byte[] newData = new byte [len - offset];
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

    public Attachment(long compressedSize, byte[] decompressedBytes) {
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

    public Payload(List<Attachment> attachments, RpcClient sender) {
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
    public void onFeedback(RpcClient sender, TStreamingFeedbackHeader header, List<byte[]> attachments)
    {
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
        return ! stash.isEof();
    }

    byte[] doRead() throws Exception {
        if (result.isCompletedExceptionally()) {
            result.get();
            return null;
        }

        return stash.pop(control);
    }

    CompletableFuture<Void> getReadyEvent() {
        return CompletableFuture.anyOf(stash.readyEvent(), result).thenAccept((unused) -> {});
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
    private static final Parser<TRspReadTableMeta> metaParser = TRspReadTableMeta.parser();

    private final TableAttachmentReader<T> reader;
    private TRspReadTableMeta metadata = null;

    public TableReaderImpl(TableAttachmentReader<T> reader) {
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
            self.metadata = RpcUtil.parseMessageBodyWithCompression(data, metaParser, Compression.None);
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

    public FileReaderImpl() {
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
            TReadFileMeta meta = RpcUtil.parseMessageBodyWithCompression(data, TReadFileMeta.parser(), Compression.None);
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
