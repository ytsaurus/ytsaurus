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
import ru.yandex.yt.ytclient.rpc.internal.Codec;
import ru.yandex.yt.ytclient.rpc.internal.RpcServiceMethodDescriptor;

interface DataSupplier extends Supplier<byte[]>
{
    byte[] get();

    default void put(byte[] data) {
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
    public void put(byte[] data) {
        messages.add(data);
    }
}

class WrappedSupplier implements DataSupplier {
    private final DataSupplier supplier;
    private final Codec codec;
    private boolean eof;

    WrappedSupplier(DataSupplier supplier, Codec codec) {
        this.supplier = supplier;
        this.codec = codec;
    }

    @Override
    public byte[] get() {
        byte[] data = supplier.get();
        eof = data == null;
        if (data != null) {
            return codec.compress(data);
        } else {
            return data;
        }
    }

    @Override
    public void put(byte[] data) {
        if (eof) {
            throw new IllegalArgumentException();
        }

        supplier.put(data);
    }

    @Override
    public boolean hasData() {
        return !eof && supplier.hasData();
    }
}

public class FileWriterImpl extends StreamBase<TRspWriteFile> implements FileWriter, RpcStreamConsumer {
    private final CompletableFuture<FileWriter> startUpload = new CompletableFuture<>();

    private final Codec codec;

    private final Object lock = new Object();
    private DataSupplier supplier = null;
    private long writePosition = 0;
    private long readPosition = 0;

    private final long windowSize;
    private final long packetSize;

    public FileWriterImpl(RpcClientStreamControl control, long windowSize, long packetSize) {
        super(control);

        this.windowSize = windowSize;
        this.packetSize = packetSize;

        this.codec = Codec.codecFor(control.compression());

        result.whenComplete((unused, ex) -> {
            if (ex != null) {
                startUpload.completeExceptionally(ex);
            }
        });
    }

    private int attachmentSize(byte[] attachment) {
        if (attachment == null) {
            return 1;
        } else {
            return attachment.length;
        }
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
                sendSize += attachmentSize(next);
            }
        }

        while (!readyToUpload.isEmpty()) {
            final List<byte[]> packet = new ArrayList<>();
            long currentPacketSize = 0;

            while (!readyToUpload.isEmpty() && currentPacketSize < packetSize) {
                byte[] data = readyToUpload.peekFirst();
                packet.add(data);
                currentPacketSize += attachmentSize(data);
                readyToUpload.removeFirst();
            }

            if (logger.isDebugEnabled()) {
                StringBuilder stringBuilder = new StringBuilder();
                stringBuilder.append("[");
                for (byte[] data : packet) {
                    stringBuilder.append(attachmentSize(data));
                    stringBuilder.append(", ");
                }
                stringBuilder.append("]");

                logger.debug("Packet: {}", stringBuilder.toString());
            }

            control.sendPayload(packet);

            synchronized (lock) {
                writePosition += currentPacketSize;
                lock.notify();
            }
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
            readPosition = header.getReadPosition();
            lock.notify();
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
        synchronized (lock) {
            while (writePosition - readPosition > windowSize) {
                try {
                    lock.wait();
                } catch (Throwable ex) {
                }

                if (result.isCompletedExceptionally()) {
                    result.join();
                }
            }
        }

        synchronized (lock) {
            supplier.put(data);
        }

        control.wakeUp();
    }

    @Override
    public CompletableFuture<Void> write(Supplier<byte[]> supplier) {
        synchronized (lock) {
            if (this.supplier != null) {
                throw new IllegalArgumentException();
            }

            this.supplier = new WrappedSupplier(supplier::get, codec);
        }

        control.wakeUp();

        return waitResult();
    }

    @Override
    public void write(byte[] data, int offset, int len) {
        synchronized (lock) {
            if (supplier == null) {
                supplier = new WrappedSupplier(new MessagesSupplier(), codec);
            }
        }

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
            lock.notify();
        }
    }

    @Override
    public void close() {
        byte[] data = null;
        write(data, 0, 0);
    }
}
