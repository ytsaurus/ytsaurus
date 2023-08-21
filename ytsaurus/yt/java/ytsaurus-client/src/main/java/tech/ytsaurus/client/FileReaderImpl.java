package tech.ytsaurus.client;

import java.util.concurrent.CompletableFuture;

import com.google.protobuf.Parser;
import tech.ytsaurus.client.rpc.Compression;
import tech.ytsaurus.client.rpc.RpcUtil;
import tech.ytsaurus.rpcproxy.TReadFileMeta;
import tech.ytsaurus.rpcproxy.TRspReadFile;

class FileReaderImpl extends StreamReaderImpl<TRspReadFile> implements FileReader {
    private long revision = -1;

    FileReaderImpl() {
    }

    @Override
    protected Parser<TRspReadFile> responseParser() {
        return TRspReadFile.parser();
    }

    @Override
    public long revision() {
        return this.revision;
    }

    public CompletableFuture<FileReader> waitMetadata() {
        FileReaderImpl self = this;
        return readHead().thenApply((data) -> {
            TReadFileMeta meta = RpcUtil.parseMessageBodyWithCompression(
                    data,
                    TReadFileMeta.parser(),
                    Compression.None
            );
            self.revision = meta.getRevision();
            return self;
        });
    }

    @Override
    public boolean canRead() {
        return doCanRead();
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
