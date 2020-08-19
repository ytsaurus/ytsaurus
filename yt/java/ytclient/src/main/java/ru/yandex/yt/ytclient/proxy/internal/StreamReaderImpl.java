package ru.yandex.yt.ytclient.proxy.internal;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import com.google.protobuf.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.yandex.bolts.collection.Tuple2;
import ru.yandex.yt.rpc.TStreamingFeedbackHeader;
import ru.yandex.yt.rpc.TStreamingPayloadHeader;
import ru.yandex.yt.ytclient.rpc.RpcClient;
import ru.yandex.yt.ytclient.rpc.RpcClientStreamControl;

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

public abstract class StreamReaderImpl<RspType extends Message> extends StreamBase<RspType> {
    private static final int MAX_WINDOW_SIZE = 16384;

    private final Stash stash = new Stash();
    private boolean started = false;
    private final SlidingWindow<Payload> window = new SlidingWindow<>(MAX_WINDOW_SIZE, payload -> {
        for (Attachment attachment : payload.getAttachments()) {
            try {
                stash.push(attachment);
            } catch (Throwable ex) {
                onError(payload.getSender(), ex);
            }
        }
    });

    StreamReaderImpl(RpcClientStreamControl control) {
        super(control);
        this.start();

        result.whenComplete((unused, ex) -> {
            if (ex != null) {
                stash.error(ex);
            }
        });
    }

    private void start() {
        if (started) {
            throw new IllegalArgumentException("already started");
        }
        started = true;
        this.control.sendEof();
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
        return result.thenAccept((unused) -> {});
    }
}
