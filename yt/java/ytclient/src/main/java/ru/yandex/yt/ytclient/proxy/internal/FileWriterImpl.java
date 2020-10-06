package ru.yandex.yt.ytclient.proxy.internal;

import java.util.concurrent.CompletableFuture;

import ru.yandex.yt.rpcproxy.TRspWriteFile;
import ru.yandex.yt.ytclient.proxy.FileWriter;
import ru.yandex.yt.ytclient.rpc.RpcClientStreamControl;
import ru.yandex.yt.ytclient.rpc.RpcMessageParser;
import ru.yandex.yt.ytclient.rpc.RpcStreamConsumer;
import ru.yandex.yt.ytclient.rpc.internal.Compression;
import ru.yandex.yt.ytclient.rpc.internal.RpcServiceMethodDescriptor;

public class FileWriterImpl extends StreamWriterImpl<TRspWriteFile> implements FileWriter, RpcStreamConsumer {
    public FileWriterImpl(RpcClientStreamControl control, Compression compression, long windowSize, long packetSize) {
        super(control, compression, windowSize, packetSize);
    }

    @Override
    protected RpcMessageParser<TRspWriteFile> responseParser() {
        return RpcServiceMethodDescriptor.makeMessageParser(TRspWriteFile.class);
    }

    public CompletableFuture<FileWriter> startUpload() {
        return startUpload.thenApply((unused) -> this);
    }

    @Override
    public boolean write(byte[] data, int offset, int len) {
        if (data != null) {
            byte[] newdata = new byte [len - offset];
            System.arraycopy(data, offset, newdata, 0, len);
            data = newdata;
        }

        return push(data);
    }
}
