package ru.yandex.yt.ytclient.proxy.internal;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;

import com.google.protobuf.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.yandex.bolts.collection.Option;
import ru.yandex.yt.rpc.TResponseHeader;
import ru.yandex.yt.ytclient.rpc.RpcClient;
import ru.yandex.yt.ytclient.rpc.RpcClientResponse;
import ru.yandex.yt.ytclient.rpc.RpcClientStreamControl;
import ru.yandex.yt.ytclient.rpc.RpcMessageParser;
import ru.yandex.yt.ytclient.rpc.RpcStreamConsumer;
import ru.yandex.yt.ytclient.rpc.internal.Codec;
import ru.yandex.yt.ytclient.rpc.internal.Compression;
import ru.yandex.yt.ytclient.rpc.internal.LazyResponse;

public abstract class StreamBase<RspType extends Message> implements RpcStreamConsumer {
    protected static final Logger logger = LoggerFactory.getLogger(StreamReaderImpl.class);

    protected final CompletableFuture<RpcClientResponse<RspType>> result = new CompletableFuture<>();

    final RpcClientStreamControl control;

    protected Compression compression;
    protected Codec codec = null;
    private int currentCodecId = -1;

    StreamBase(RpcClientStreamControl control) {
        this.control = control;
        this.control.subscribe(this);
    }

    protected abstract RpcMessageParser<RspType> responseParser();

    void maybeReinitCodec(int codecId) {
        if (currentCodecId != codecId) {
            compression = Compression.fromValue(codecId);
            codec = Codec.codecFor(compression);
            currentCodecId = codecId;
        }
    }

    List<byte[]> decomressedAttachments(int codecId, List<byte[]> attachments) {
        maybeReinitCodec(codecId);

        List<byte[]> decompressed;
        if (currentCodecId == 0) {
            decompressed = attachments;
        } else {
            decompressed = new ArrayList<>();
            for (byte[] attachment : attachments) {
                if (attachment == null) {
                    decompressed.add(attachment);
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

            attachments = decomressedAttachments(header.getCodec(), attachments);

            result.complete(new LazyResponse<>(responseParser(), attachments.get(0),
                    new ArrayList<>(attachments.subList(1, attachments.size())), sender,
                    Option.of(header)));
        }
    }

    @Override
    public void onError(RpcClient sender, Throwable error) {
        logger.error("Error", error);
        result.completeExceptionally(error);
    }

    @Override
    public void onCancel(RpcClient sender, CancellationException cancel) {
        result.completeExceptionally(cancel);
    }

    protected CompletableFuture<Void> waitResult() {
        return result.thenApply((unused) -> null);
    }

    public void cancel() {
        control.cancel();
    }
}
