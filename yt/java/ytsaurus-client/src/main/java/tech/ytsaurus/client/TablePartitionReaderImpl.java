package tech.ytsaurus.client;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Consumer;

import javax.annotation.Nullable;

import com.google.protobuf.Parser;
import tech.ytsaurus.client.rpc.Compression;
import tech.ytsaurus.client.rpc.RpcUtil;
import tech.ytsaurus.rpcproxy.TRspReadTablePartition;
import tech.ytsaurus.rpcproxy.TRspReadTablePartitionMeta;

public class TablePartitionReaderImpl<T> extends StreamReaderImpl<TRspReadTablePartition>
        implements AsyncReader<T> {
    private static final Parser<TRspReadTablePartitionMeta> META_PARSER = TRspReadTablePartitionMeta.parser();
    private final TableAttachmentReader<T> reader;
    protected TRspReadTablePartitionMeta metadata = null;
    @Nullable
    protected ApiServiceTransaction transaction;

    TablePartitionReaderImpl(TableAttachmentReader<T> reader) {
        this.reader = reader;
    }

    public CompletableFuture<AsyncReader<T>> waitMetadata() {
        TablePartitionReaderImpl<T> self = this;
        return readHead().thenApply((data) -> {
            self.metadata = RpcUtil.parseMessageBodyWithCompression(data, META_PARSER, Compression.None);
            return self;
        });
    }

    @Override
    protected Parser<TRspReadTablePartition> responseParser() {
        return TRspReadTablePartition.parser();
    }

    @Override
    public CompletableFuture<Void> acceptAllAsync(Consumer<? super T> consumer, Executor executor) {
        return next().thenComposeAsync(rows -> {
            if (rows == null) {
                return CompletableFuture.completedFuture(null);
            }
            rows.forEach(consumer);
            return acceptAllAsync(consumer, executor);
        }, executor);
    }

    @Override
    public CompletableFuture<List<T>> next() {
        try {
            List<T> rows = reader.parse(doRead());
            if (rows != null) {
                return CompletableFuture.completedFuture(rows);
            }
            return getReadyEvent().thenCompose(unused -> {
                if (doCanRead()) {
                    return next();
                } else {
                    return CompletableFuture.completedFuture(null);
                }
            });
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws IOException {
        control.cancel();
    }
}
