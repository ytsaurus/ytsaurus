package ru.yandex.yt.ytclient.proxy.internal;

import java.util.concurrent.CompletableFuture;

import ru.yandex.yt.rpcproxy.TReadFileMeta;
import ru.yandex.yt.rpcproxy.TRspReadFile;
import ru.yandex.yt.ytclient.proxy.FileReader;
import ru.yandex.yt.ytclient.rpc.RpcClientStreamControl;
import ru.yandex.yt.ytclient.rpc.RpcMessageParser;
import ru.yandex.yt.ytclient.rpc.RpcUtil;
import ru.yandex.yt.ytclient.rpc.internal.Compression;
import ru.yandex.yt.ytclient.rpc.internal.RpcServiceMethodDescriptor;

public class FileReaderImpl extends StreamReaderImpl<TRspReadFile> implements FileReader {
    private final static RpcMessageParser<TReadFileMeta> metaParser = RpcServiceMethodDescriptor.makeMessageParser(TReadFileMeta.class);

    private long revision = -1;

    public FileReaderImpl(RpcClientStreamControl control) {
        super(control);
    }

    @Override
    protected RpcMessageParser<TRspReadFile> responseParser() {
        return RpcServiceMethodDescriptor.makeMessageParser(TRspReadFile.class);
    }

    @Override
    public long revision() {
        return this.revision;
    }

    public CompletableFuture<FileReader> waitMetadata() {
        FileReaderImpl self = this;
        return readHead().thenApply((data) -> {
            TReadFileMeta meta = RpcUtil.parseMessageBodyWithCompression(data, metaParser, Compression.None);
            self.revision = meta.getRevision();
            return self;
        });
    }

    @Override
    public byte[] read() throws Exception {
        return doRead();
    }

    @Override
    public CompletableFuture<Void> close() {
        return doClose();
    }

    @Override
    public CompletableFuture<Void> readyEvent() {
        return getReadyEvent();
    }
}
