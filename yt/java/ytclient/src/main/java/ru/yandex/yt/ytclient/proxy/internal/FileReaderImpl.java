package ru.yandex.yt.ytclient.proxy.internal;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import ru.yandex.yt.rpcproxy.TReadFileMeta;
import ru.yandex.yt.rpcproxy.TRspReadFile;
import ru.yandex.yt.ytclient.proxy.FileReader;
import ru.yandex.yt.ytclient.rpc.RpcClientStreamControl;
import ru.yandex.yt.ytclient.rpc.RpcMessageParser;
import ru.yandex.yt.ytclient.rpc.RpcUtil;
import ru.yandex.yt.ytclient.rpc.internal.RpcServiceMethodDescriptor;

public class FileReaderImpl extends StreamReaderImpl<TRspReadFile> implements FileReader {
    private final static RpcMessageParser<TReadFileMeta> metaParser = RpcServiceMethodDescriptor.makeMessageParser(TReadFileMeta.class);

    private CompletableFuture<Long> revision = new CompletableFuture<>();

    public FileReaderImpl(RpcClientStreamControl control) {
        super(control);
    }

    @Override
    protected RpcMessageParser<TRspReadFile> responseParser() {
        return RpcServiceMethodDescriptor.makeMessageParser(TRspReadFile.class);
    }

    @Override
    public CompletableFuture<Long> revision() {
        return this.revision;
    }

    @Override
    public void read(Consumer<byte[]> consumer) {
        doRead((next) -> {
            if (!revision.isDone()) {
                TReadFileMeta meta = RpcUtil.parseMessageBodyWithCompression(next, metaParser, compression);
                revision.complete(meta.getRevision());
            } else {
                consumer.accept(next);
            }
        });
    }

    @Override
    public byte[] read() throws Exception {
        byte[] next = doRead();

        if (!revision.isDone()) {
            TReadFileMeta meta = RpcUtil.parseMessageBodyWithCompression(next, metaParser, compression);
            revision.complete(meta.getRevision());
            return doRead();
        } else {
            return next;
        }
    }
}
