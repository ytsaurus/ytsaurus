package ru.yandex.yt.ytclient.proxy.internal;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.yandex.bolts.collection.Option;
import ru.yandex.yt.rpc.TResponseHeader;
import ru.yandex.yt.rpc.TStreamingFeedbackHeader;
import ru.yandex.yt.rpc.TStreamingPayloadHeader;
import ru.yandex.yt.rpcproxy.TReadFileMeta;
import ru.yandex.yt.rpcproxy.TRspReadFile;
import ru.yandex.yt.ytclient.proxy.FileReader;
import ru.yandex.yt.ytclient.rpc.RpcClient;
import ru.yandex.yt.ytclient.rpc.RpcClientResponse;
import ru.yandex.yt.ytclient.rpc.RpcClientStreamControl;
import ru.yandex.yt.ytclient.rpc.RpcMessageParser;
import ru.yandex.yt.ytclient.rpc.RpcStreamConsumer;
import ru.yandex.yt.ytclient.rpc.RpcUtil;
import ru.yandex.yt.ytclient.rpc.internal.Codec;
import ru.yandex.yt.ytclient.rpc.internal.Compression;
import ru.yandex.yt.ytclient.rpc.internal.LazyResponse;
import ru.yandex.yt.ytclient.rpc.internal.RpcServiceMethodDescriptor;

public class FileReaderImpl implements RpcStreamConsumer, FileReader {
    private static final Logger logger = LoggerFactory.getLogger(FileReaderImpl.class);

    private final static RpcMessageParser<TRspReadFile> responseParser = RpcServiceMethodDescriptor.makeMessageParser(TRspReadFile.class);
    private final static RpcMessageParser<TReadFileMeta> metaParser = RpcServiceMethodDescriptor.makeMessageParser(TReadFileMeta.class);

    private final CompletableFuture<RpcClientResponse<TRspReadFile>> result;

    private final RpcClientStreamControl control;
    private long offset = 0;
    private boolean started = false;

    private Optional<Long> revision = Optional.empty();

    private final Object lock = new Object();

    private Stash stash = null;

    class StashedMessage {
        byte [] data;
        long offset;

        StashedMessage(byte [] data, long offset) {
            this.data = data;
            this.offset = offset;
        }
    }

    interface Stash {
        void push(byte [] data, long offset);
        StashedMessage read() throws Exception;
        void commit(RpcClientStreamControl control);
    }

    class AsyncStash implements Stash {
        private final Consumer<byte[]> consumer;
        private long offset = 0;

        AsyncStash(Consumer<byte[]> consumer) {
            this.consumer = consumer;
        }

        @Override
        public void push(byte [] data, long offset) {
            this.offset = offset;
            if (data != null) {
                consumer.accept(data);
            }
        }

        @Override
        public StashedMessage read()  {
            throw new IllegalArgumentException();
        }

        @Override
        public void commit(RpcClientStreamControl control) {
            control.feedback(offset);
        }
    }

    class SyncStash implements Stash {
        private final LinkedList<StashedMessage> messages = new LinkedList<>();

        @Override
        public void push(byte [] data, long offset) {
            synchronized (messages) {
                messages.push(new StashedMessage(data, offset));
                messages.notify();
            }
        }

        @Override
        public StashedMessage read() throws Exception {
            synchronized (messages) {
                while (messages.isEmpty()) {
                    messages.wait();
                }

                return messages.removeFirst();
            }
        }

        @Override
        public void commit(RpcClientStreamControl control) { }
    }

    public FileReaderImpl(RpcClientStreamControl control) {
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

    @Override
    public void onFeedback(RpcClient sender, TStreamingFeedbackHeader header, List<byte[]> attachments) {
        logger.debug("feedback {}", RpcUtil.fromProto(header.getRequestId()));
    }

    @Override
    public void onPayload(RpcClient sender, TStreamingPayloadHeader header, List<byte[]> attachments) {
        if (attachments.isEmpty()) {
            onError(sender, new IllegalArgumentException("Empty attachments"));
            return;
        }

        Compression compression = Compression.fromValue(header.getCodec());
        Codec codec = Codec.codecFor(compression);

        if (!revision.isPresent()) {
            byte[] revisionData = attachments.get(0);

            TReadFileMeta meta = RpcUtil.parseMessageBodyWithCompression(revisionData, metaParser, compression);
            revision = Optional.of(meta.getRevision());

            logger.debug("Read revision {}", revision.get());

            attachments = attachments.subList(1, attachments.size());
            offset += revisionData.length;
        }

        for (byte[] attachment : attachments) {
            if (attachment != null) {

                byte [] decompressed = codec.decompress(attachment);

                offset += attachment.length;

                synchronized (lock) {
                    stash.push(decompressed, offset);
                }

            } else {
                offset += 1;

                synchronized (lock) {
                    stash.push(null, offset);
                }
            }
        }

        if (attachments.isEmpty()) {
            control.feedback(offset);
        } else {
            synchronized (lock) {
                stash.commit(control);
            }
        }
    }

    @Override
    public void onResponse(RpcClient sender, TResponseHeader header, List<byte[]> attachments) {
        logger.debug("Response");

        if (!result.isDone()) {
            if (attachments.size() < 1 || attachments.get(0) == null) {
                throw new IllegalStateException("Received response without a body");
            }

            result.complete(new LazyResponse<>(responseParser, attachments.get(0),
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

    public void cancel() {
        control.cancel();
    }

    @Override
    public long getRevision() throws Exception {
        if (revision.isPresent()) {
            return revision.get();
        }
        throw new IllegalArgumentException("Revision is not ready");
    }

    @Override
    public void read(Consumer<byte[]> consumer) {
        synchronized (lock) {
            start();
            this.stash = new AsyncStash(consumer);
        }
    }

    @Override
    public byte[] read() throws Exception {
        synchronized (lock) {
            if (!started) {
                start();
                this.stash = new SyncStash();
            }
        }

        if (result.isCompletedExceptionally()) {
            result.get();
            return null;
        }

        StashedMessage message = stash.read();

        if (result.isCompletedExceptionally()) {
            result.get();
            return null;
        }

        control.feedback(message.offset);
        return message.data;
    }

    @Override
    public CompletableFuture<Void> waitResult() {
        return result.thenApply((unused) -> null);
    }
}
