package ru.yandex.yt.ytclient.proxy.internal;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import ru.yandex.yt.rpc.TStreamingFeedbackHeader;
import ru.yandex.yt.rpc.TStreamingPayloadHeader;
import ru.yandex.yt.rpcproxy.TRspWriteFile;
import ru.yandex.yt.ytclient.proxy.FileWriter;
import ru.yandex.yt.ytclient.rpc.RpcClient;
import ru.yandex.yt.ytclient.rpc.RpcClientStreamControl;
import ru.yandex.yt.ytclient.rpc.RpcMessageParser;
import ru.yandex.yt.ytclient.rpc.RpcStreamConsumer;
import ru.yandex.yt.ytclient.rpc.RpcUtil;
import ru.yandex.yt.ytclient.rpc.internal.Codec;
import ru.yandex.yt.ytclient.rpc.internal.Compression;
import ru.yandex.yt.ytclient.rpc.internal.RpcServiceMethodDescriptor;

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

public class FileWriterImpl extends StreamBase<TRspWriteFile> implements FileWriter, RpcStreamConsumer {
    private final CompletableFuture<FileWriter> startUpload = new CompletableFuture<>();

    private final Object lock = new Object();
    private final DataSupplier supplier;

    private CompletableFuture<Void> readyEvent;
    private final CompletableFuture<Void> completedFuture = CompletableFuture.completedFuture(null);
    private long writePosition = 0;
    private long readPosition = 0;

    private final long windowSize;
    private final long packetSize;

    public FileWriterImpl(RpcClientStreamControl control, long windowSize, long packetSize) {
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
    protected RpcMessageParser<TRspWriteFile> responseParser() {
        return RpcServiceMethodDescriptor.makeMessageParser(TRspWriteFile.class);
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

    @Override
    public void onPayload(RpcClient sender, TStreamingPayloadHeader header, List<byte[]> attachments) {
        if (attachments.size() != 1) {
            throw new IllegalArgumentException("protocol error");
        }

        if (attachments.get(0) != null) {
            throw new IllegalArgumentException("protocol error");
        }

        if (!startUpload.isDone()) {
            startUpload.complete(this);
        } else {
            throw new IllegalArgumentException("protocol error");
        }

        control.feedback(1);
    }

    @Override
    public void onWakeup() {
        uploadSome();
    }

    public CompletableFuture<FileWriter> startUpload() {
        return startUpload;
    }

    private void push(byte[] data) {
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
    public void write(byte[] data, int offset, int len) {
        if (data != null) {
            byte[] newdata = new byte [len - offset];
            System.arraycopy(data, offset, newdata, 0, len);
            data = newdata;
        }

        push(data);

        if (data == null) {
            result.join();
        }
    }

    @Override
    public void onError(RpcClient sender, Throwable error) {
        super.onError(sender, error);

        synchronized (lock) {
            reinitReadyEvent();
        }
    }

    @Override
    public CompletableFuture<Void> close() {
        push(null);

        return result.thenApply((unused) -> null);
    }
}
