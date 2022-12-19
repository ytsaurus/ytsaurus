package tech.ytsaurus.client;

import java.util.concurrent.CompletableFuture;

import com.google.protobuf.Parser;
import tech.ytsaurus.client.rpc.RpcStreamConsumer;
import tech.ytsaurus.rpcproxy.TRspWriteFile;

class FileWriterImpl extends StreamWriterImpl<TRspWriteFile> implements FileWriter, RpcStreamConsumer {
    FileWriterImpl(long windowSize, long packetSize) {
        super(windowSize, packetSize);
    }

    @Override
    protected Parser<TRspWriteFile> responseParser() {
        return TRspWriteFile.parser();
    }

    public CompletableFuture<FileWriter> startUpload() {
        return startUpload.thenApply((unused) -> this);
    }

    @Override
    public boolean write(byte[] data, int offset, int len) {
        if (data != null) {
            byte[] newData = new byte[len - offset];
            System.arraycopy(data, offset, newData, 0, len);
            data = newData;
        }

        return push(data);
    }
}
