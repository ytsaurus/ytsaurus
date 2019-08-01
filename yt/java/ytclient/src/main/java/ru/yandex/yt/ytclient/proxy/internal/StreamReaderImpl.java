package ru.yandex.yt.ytclient.proxy.internal;

import com.google.protobuf.Message;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.yandex.bolts.collection.Option;
import ru.yandex.yt.rpc.TResponseHeader;
import ru.yandex.yt.rpc.TStreamingFeedbackHeader;
import ru.yandex.yt.rpc.TStreamingPayloadHeader;
import ru.yandex.yt.ytclient.rpc.RpcClient;
import ru.yandex.yt.ytclient.rpc.RpcClientResponse;
import ru.yandex.yt.ytclient.rpc.RpcClientStreamControl;
import ru.yandex.yt.ytclient.rpc.RpcMessageParser;
import ru.yandex.yt.ytclient.rpc.RpcStreamConsumer;
import ru.yandex.yt.ytclient.rpc.internal.Codec;
import ru.yandex.yt.ytclient.rpc.internal.Compression;
import ru.yandex.yt.ytclient.rpc.internal.LazyResponse;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

public abstract class StreamReaderImpl<RspType extends Message> implements RpcStreamConsumer {
    protected static final Logger logger = LoggerFactory.getLogger(StreamReaderImpl.class);
    protected final RpcClientStreamControl control;

    private final CompletableFuture<RpcClientResponse<RspType>> result;

    private final Object lock = new Object();
    private StreamStash<byte[]> stash;
    private boolean started = false;
    private long offset = 0;

    public StreamReaderImpl(RpcClientStreamControl control) {
        this.control = control;
        this.control.subscribe(this);
        this.result = new CompletableFuture<>();
    }

    private void start() {
        if (started) {
            throw new IllegalArgumentException("already started");
        }
        started = true;
        this.control.sendEof();
    }

    protected abstract RpcMessageParser<RspType> responseParser();

    @Override
    public void onFeedback(RpcClient sender, TStreamingFeedbackHeader header, List<byte[]> attachments)
    {
    }

    protected Compression compression;
    private Codec codec = null;
    private int currentCodecId = -1;

    private void maybeReinitCodec(int codecId) {
        if (currentCodecId != codecId) {
            compression = Compression.fromValue(codecId);
            Codec.codecFor(compression);
            currentCodecId = codecId;
        }
    }

    private List<byte[]> decomressedAttachments(int codecId, List<byte[]> attachments) {
        maybeReinitCodec(codecId);

        List<byte[]> decompressed;
        if (currentCodecId == 0) {
            decompressed = attachments;
        } else {
            decompressed = new ArrayList<>();
            for (byte[] attachment : attachments) {
                decompressed.add(codec.decompress(attachment));
            }
        }

        return decompressed;
    }

    @Override
    public void onPayload(RpcClient sender, TStreamingPayloadHeader header, List<byte[]> attachments) {
        if (attachments.isEmpty()) {
            onError(sender, new IllegalArgumentException("Empty attachments"));
            return;
        }

        attachments = decomressedAttachments(header.getCodec(), attachments);

        for (byte [] attachment : attachments) {
            long size = attachment == null
                    ? 1
                    : attachment.length;

            offset += size;

            synchronized (lock) {
                stash.push(attachment, offset);
            }
        }

        synchronized (lock) {
            stash.commit(control);
        }
    }

    @Override
    public void onResponse(RpcClient sender, TResponseHeader header, List<byte[]> attachments) {
        if (!result.isDone()) {
            if (attachments.size() < 1 || attachments.get(0) == null) {
                throw new IllegalStateException("Received response without a body");
            }

            attachments = decomressedAttachments(header.getCodec(), attachments);

            result.complete(new LazyResponse<>(responseParser(), attachments.get(0),
                    new ArrayList<>(attachments.subList(1, attachments.size())), sender,
                    Option.of(header)));
        }
    }

    @Override
    public void onError(RpcClient sender, Throwable error) {
        logger.error("Error", error);

        if (!result.isDone()) {
            result.completeExceptionally(error);
        }

        synchronized (lock) {
            if (stash != null) {
                stash.push(null, 0);
            }
        }
    }

    public CompletableFuture<Void> waitResult() {
        return result.thenApply((unused) -> null);
    }

    public void cancel() {
        control.cancel();
    }

    byte[] doRead() throws Exception {
        synchronized (lock) {
            if (!started) {
                start();
                this.stash = StreamStash.syncStash();
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

    CompletableFuture<Void> doRead(Consumer<byte[]> consumer) {
        synchronized (lock) {
            start();
            this.stash = StreamStash.asyncStash(consumer);
        }

        return waitResult();
    }
}
