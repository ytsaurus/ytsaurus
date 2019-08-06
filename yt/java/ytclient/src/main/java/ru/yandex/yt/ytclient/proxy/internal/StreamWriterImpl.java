package ru.yandex.yt.ytclient.proxy.internal;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import com.google.protobuf.Message;
import ru.yandex.bolts.collection.Cf;
import ru.yandex.yt.rpc.TStreamingFeedbackHeader;
import ru.yandex.yt.rpc.TStreamingPayloadHeader;
import ru.yandex.yt.ytclient.proxy.StreamWriter;
import ru.yandex.yt.ytclient.rpc.RpcClient;
import ru.yandex.yt.ytclient.rpc.RpcClientStreamControl;
import ru.yandex.yt.ytclient.rpc.RpcStreamConsumer;
import ru.yandex.yt.ytclient.rpc.RpcUtil;
import ru.yandex.yt.ytclient.rpc.internal.Codec;
import ru.yandex.yt.ytclient.rpc.internal.Compression;

interface DataSupplier extends Supplier<byte[]>
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
    private final Codec outputCodec;
    private boolean eof;

    WrappedSupplier(DataSupplier supplier, Codec inputCodec, Codec outputCodec) {
        this.supplier = supplier;
        this.inputCodec = inputCodec;
        this.outputCodec = outputCodec;
    }

    @Override
    public byte[] get() {
        byte[] data = supplier.get();
        eof = data == null;
        if (data != null) {
            return outputCodec.compress(data);
        } else {
            return data;
        }
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

abstract public class StreamWriterImpl<T extends Message> extends StreamBase<T> implements RpcStreamConsumer, StreamWriter {
    final CompletableFuture<List<byte[]>> startUpload = new CompletableFuture<>();

    private final Object lock = new Object();
    private final DataSupplier supplier;

    private CompletableFuture<Void> readyEvent;
    private final CompletableFuture<Void> completedFuture = CompletableFuture.completedFuture(null);
    private long writePosition = 0;
    private long readPosition = 0;

    private final long windowSize;
    private final long packetSize;


    StreamWriterImpl(RpcClientStreamControl control, long windowSize, long packetSize) {
        super(control);


        this.windowSize = windowSize;
        this.packetSize = packetSize;

        Codec codec = Codec.codecFor(control.compression());

        this.supplier = new WrappedSupplier(new MessagesSupplier(), codec, Codec.codecFor(Compression.None));

        initReadyEvent();

        result.whenComplete((unused, ex) -> {
            if (ex != null) {
                startUpload.completeExceptionally(ex);
            }
        });
    }

    private void initReadyEvent() {
        this.readyEvent = new CompletableFuture<>();
    }

    private void reinitReadyEvent() {
        this.readyEvent.complete(null);
        initReadyEvent();
    }

    private void uploadSome() {
        synchronized (lock) {
            if (!supplier.hasData() || writePosition - readPosition >= windowSize) {
                return;
            }
        }

        final LinkedList<byte[]> readyToUpload = new LinkedList<>();

        long sendSize = 0;

        synchronized (lock) {
            while (supplier.hasData() && sendSize + writePosition - readPosition < windowSize) {
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

            if (logger.isDebugEnabled()) {
                StringBuilder stringBuilder = new StringBuilder();
                stringBuilder.append("[");
                for (byte[] data : packet) {
                    stringBuilder.append(RpcUtil.attachmentSize(data));
                    stringBuilder.append(", ");
                }
                stringBuilder.append("]");

                logger.debug("Packet: {} {}", stringBuilder.toString(), writePosition - readPosition);
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
            long oldReadPosition = readPosition;
            readPosition = header.getReadPosition();
            if (writePosition - oldReadPosition >= windowSize && writePosition - readPosition < windowSize) {
                reinitReadyEvent();
            }
        }

        uploadSome();
    }

    private List<byte[]> payloadAttachments = Cf.linkedList();
    private long payloadOffset = 0;

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

    boolean push(byte[] data) {
        synchronized (lock) {
            if (writePosition - readPosition >= windowSize) {
                return false;
            }
        }

        if (result.isCompletedExceptionally()) {
            result.join();
        }

        synchronized (lock) {
            long oldWritePosition = writePosition;

            writePosition += supplier.put(data);

            if (oldWritePosition - readPosition >= windowSize && writePosition - readPosition < windowSize) {
                reinitReadyEvent();
            }
        }

        control.wakeUp();

        return true;
    }

    @Override
    public void onError(RpcClient sender, Throwable error) {
        super.onError(sender, error);

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
    public CompletableFuture<Void> close() {
        push(null);

        return result.thenApply((unused) -> null);
    }
}
