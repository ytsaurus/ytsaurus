package tech.ytsaurus.client;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.Nullable;

import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ytsaurus.client.rpc.Codec;
import tech.ytsaurus.client.rpc.Compression;
import tech.ytsaurus.client.rpc.LazyResponse;
import tech.ytsaurus.client.rpc.RpcClient;
import tech.ytsaurus.client.rpc.RpcClientResponse;
import tech.ytsaurus.client.rpc.RpcClientStreamControl;
import tech.ytsaurus.client.rpc.RpcStreamConsumer;
import tech.ytsaurus.client.rpc.RpcUtil;
import tech.ytsaurus.lang.NonNullApi;
import tech.ytsaurus.rpc.TResponseHeader;
import tech.ytsaurus.rpc.TStreamingFeedbackHeader;
import tech.ytsaurus.rpc.TStreamingPayloadHeader;
import tech.ytsaurus.rpcproxy.TRspWriteTable;


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

    private final LinkedList<AbstractMap.SimpleEntry<byte[], Long>> attachments = new LinkedList<>();

    void push(Attachment attachment) throws Throwable {
        synchronized (attachments) {
            if (ex != null) {
                throw ex;
            }

            boolean needWakeup = attachments.isEmpty() && !eof;
            offset += attachment.getCompressedSize();

            attachments.addLast(new AbstractMap.SimpleEntry(attachment.getDecompressedBytes(), offset));

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
                AbstractMap.SimpleEntry<byte[], Long> message = attachments.removeFirst();
                control.feedback(message.getValue());
                eof = message.getKey() == null;
                return message.getKey();
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
        for (var attachment : attachments) {
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
