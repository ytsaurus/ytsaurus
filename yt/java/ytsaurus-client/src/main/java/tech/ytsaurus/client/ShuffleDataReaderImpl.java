package tech.ytsaurus.client;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Consumer;

import com.google.protobuf.Parser;
import tech.ytsaurus.client.rows.UnversionedRow;
import tech.ytsaurus.rpcproxy.TRspReadShuffleData;

public class ShuffleDataReaderImpl extends StreamReaderImpl<TRspReadShuffleData>
        implements AsyncReader<UnversionedRow> {

    private final ShuffleDataRowsetReader reader = new ShuffleDataRowsetReader();

    public CompletableFuture<AsyncReader<UnversionedRow>> startRead() {
        ShuffleDataReaderImpl self = this;
        return getReadyEvent().thenApply((unused) -> self);
    }

    @Override
    protected Parser<TRspReadShuffleData> responseParser() {
        return TRspReadShuffleData.parser();
    }

    @Override
    public CompletableFuture<Void> acceptAllAsync(Consumer<? super UnversionedRow> consumer, Executor executor) {
        return next().thenComposeAsync(rows -> {
            if (rows == null) {
                return CompletableFuture.completedFuture(null);
            }
            rows.forEach(consumer);
            return acceptAllAsync(consumer, executor);
        }, executor);
    }

    @Override
    public CompletableFuture<List<UnversionedRow>> next() {
        try {
            List<UnversionedRow> rows = reader.parse(doRead());
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
