package ru.yandex.yt.ytclient.proxy.internal;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import com.google.protobuf.Message;

import ru.yandex.bolts.collection.Tuple2;
import ru.yandex.yt.rpc.TStreamingFeedbackHeader;
import ru.yandex.yt.rpc.TStreamingPayloadHeader;
import ru.yandex.yt.ytclient.rpc.RpcClient;
import ru.yandex.yt.ytclient.rpc.RpcClientStreamControl;

class Stash {
    private final CompletableFuture<Void> completedFuture = CompletableFuture.completedFuture(null);
    private CompletableFuture<Void> readyEvent = new CompletableFuture<>();
    private Throwable ex = null;

    private final LinkedList<Tuple2<byte[], Long>> attachments = new LinkedList<>();

    void push(byte[] attachment, long offset) throws Throwable {
        synchronized (attachments) {
            if (ex != null) {
                throw ex;
            }

            boolean wasEmpty = attachments.isEmpty();

            attachments.push(new Tuple2<>(attachment, offset));

            if (wasEmpty) {
                this.readyEvent.complete(null);
                readyEvent = new CompletableFuture<>();
            }
        }
    }

    byte[] pop(RpcClientStreamControl control) {
        synchronized (attachments) {
            Tuple2<byte[], Long> message = attachments.removeFirst();
            control.feedback(message._2);
            return message._1;
        }
    }

    CompletableFuture<Void> readyEvent() {
        synchronized (attachments) {
            if (attachments.isEmpty()) {
                return this.readyEvent;
            } else {
                return completedFuture;
            }
        }
    }

    void error(Throwable ex) {
        synchronized (attachments) {
            this.ex = ex;

            if (!this.readyEvent.isDone()) {
                this.readyEvent.completeExceptionally(ex);
            }
        }
    }
}

public abstract class StreamReaderImpl<RspType extends Message> extends StreamBase<RspType> {
    private Stash stash = new Stash();
    private boolean started = false;
    private long offset = 0;
    private int currentSequenceNumber = -1;

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
        if (currentSequenceNumber >= 0 && sequenceNumber - currentSequenceNumber != 1) {
            throw new IllegalArgumentException("protocol error");
        }
        currentSequenceNumber = sequenceNumber;

        maybeReinitCodec(header.getCodec());

        try {
            for (byte[] attachment : attachments) {
                long size = attachment == null
                        ? 1
                        : attachment.length;

                offset += size;

                byte[] attachmentDecompressed = attachment != null
                        ? codec.decompress(attachment)
                        : null;

                stash.push(attachmentDecompressed, offset);
            }
        } catch (Throwable ex) {
            onError(sender, ex);
        }
    }

    CompletableFuture<byte[]> readHead() {
        return getReadyEvent().thenApply((unused) -> stash.pop(control));
    }

    byte[] doRead() throws Exception {
        if (result.isCompletedExceptionally()) {
            result.get();
            return null;
        }

        return stash.pop(control);
    }

    CompletableFuture<Void> getReadyEvent() {
        return stash.readyEvent();
    }

    CompletableFuture<Void> doClose() {
        return result.thenAccept((unused) -> {});
    }
}
