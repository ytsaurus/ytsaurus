package tech.ytsaurus.client;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import com.google.protobuf.Parser;
import tech.ytsaurus.client.rows.UnversionedRow;
import tech.ytsaurus.client.rows.UnversionedRowSerializer;
import tech.ytsaurus.client.rpc.RpcStreamConsumer;
import tech.ytsaurus.core.tables.ColumnValueType;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.rpcproxy.TRspWriteShuffleData;

public class ShuffleDataWriterImpl extends StreamWriterImpl<TRspWriteShuffleData>
        implements AsyncWriter<UnversionedRow>, RpcStreamConsumer {

    private final TableRowsWireSerializer<UnversionedRow> tableRowsSerializer =
            new TableRowsWireSerializer<>(new UnversionedRowSerializer());

    private final TableSchema schema;

    public ShuffleDataWriterImpl(long windowSize, long packetSize, String partitionColumnName) {
        super(windowSize, packetSize);
        this.schema = TableSchema.builder()
                .addValue(partitionColumnName, ColumnValueType.INT64)
                .addValue("data", ColumnValueType.STRING)
                .build();
    }

    @Override
    protected Parser<TRspWriteShuffleData> responseParser() {
        return TRspWriteShuffleData.parser();
    }

    public CompletableFuture<AsyncWriter<UnversionedRow>> startUpload() {
        return startUpload.thenApply((unused) -> this);
    }

    @Override
    public CompletableFuture<Void> write(List<UnversionedRow> rows) {
        try {
            var descriptor = tableRowsSerializer.getCurrentRowsetDescriptor(schema);
            tableRowsSerializer.write(rows, schema, descriptor);
            byte[] serializedRows =
                    TableRowsSerializerUtil.serializeRowsWithDescriptor(tableRowsSerializer, descriptor);
            if (push(serializedRows)) {
                return CompletableFuture.completedFuture(null);
            }
            return readyEvent().thenCompose(unused -> write(rows));
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public CompletableFuture<?> finish() {
        return close();
    }
}
