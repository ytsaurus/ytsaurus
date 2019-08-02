package ru.yandex.yt.ytclient.proxy.internal;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import com.google.protobuf.Message;

import ru.yandex.yt.rpc.TStreamingFeedbackHeader;
import ru.yandex.yt.rpc.TStreamingPayloadHeader;
import ru.yandex.yt.ytclient.rpc.RpcClient;
import ru.yandex.yt.ytclient.rpc.RpcClientStreamControl;

public abstract class StreamReaderImpl<RspType extends Message> extends StreamBase<RspType> {
    private final Object lock = new Object();
    private StreamStash<byte[]> stash;
    private boolean started = false;
    private boolean syncReadStarted = false;
    private long offset = 0;
    private int currentSequenceNumber = -1;

    StreamReaderImpl(RpcClientStreamControl control) {
        super(control);
        this.start();
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

        for (byte [] attachment : attachments) {
            long size = attachment == null
                    ? 1
                    : attachment.length;

            offset += size;

            synchronized (lock) {
                byte [] attachmentDecompressed = attachment != null
                        ? codec.decompress(attachment)
                        : null;
                stash.push(attachmentDecompressed, offset);
            }
        }

        synchronized (lock) {
            stash.commit(control);
        }
    }

    @Override
    public void onError(RpcClient sender, Throwable error) {
        super.onError(sender, error);

        synchronized (lock) {
            stash.push(null, 0);
        }
    }

    byte[] doRead() throws Exception {
        synchronized (lock) {
            if (!syncReadStarted) {
                this.stash = StreamStash.syncStash(stash.messages());
                syncReadStarted = true;
            }
        }


        if (result.isCompletedExceptionally()) {
            result.get();
            return null;
        }

        StashedMessage<byte[]> message = stash.read();

        if (result.isCompletedExceptionally()) {
            result.get();
            return null;
        }

        control.feedback(message.offset);
        return message.data;
    }

    CompletableFuture<Void> doRead(Function<byte[], Boolean> function) {
        synchronized (lock) {
            if (syncReadStarted) {
                throw new IllegalArgumentException();
            }
            this.stash = StreamStash.asyncStash(function,
                    stash != null
                            ? stash.messages()
                            : new LinkedList<>()
            );
        }

        return waitResult();
    }

    CompletableFuture<byte[]> readHead() {
        CompletableFuture<byte[]> headResult = new CompletableFuture<>();

        CompletableFuture<Void> maybeError = doRead((data) -> {
            if (headResult.isDone()) {
                return false;
            }
            headResult.complete(data);
            return true;
        });

        maybeError.whenComplete((unused, ex) -> {
            if (ex != null) {
                headResult.completeExceptionally(ex);
            }
        });

        return headResult;
    }
}
