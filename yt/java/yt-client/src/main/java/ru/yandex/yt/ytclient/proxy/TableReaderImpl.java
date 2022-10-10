package ru.yandex.yt.ytclient.proxy;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Consumer;

import javax.annotation.Nullable;

import NYT.NChunkClient.NProto.DataStatistics;
import com.google.protobuf.Parser;

import ru.yandex.inside.yt.kosher.impl.ytree.object.YTreeRowSerializer;
import ru.yandex.inside.yt.kosher.impl.ytree.object.YTreeSerializer;
import ru.yandex.yt.rpcproxy.TRspReadTable;
import ru.yandex.yt.rpcproxy.TRspReadTableMeta;
import ru.yandex.yt.ytclient.SerializationResolver;
import ru.yandex.yt.ytclient.object.MappedRowsetDeserializer;
import ru.yandex.yt.ytclient.proxy.internal.TableAttachmentReader;
import ru.yandex.yt.ytclient.proxy.internal.TableAttachmentWireProtocolReader;
import ru.yandex.yt.ytclient.request.ReadTable;
import ru.yandex.yt.ytclient.rpc.RpcUtil;
import ru.yandex.yt.ytclient.rpc.internal.Compression;
import ru.yandex.yt.ytclient.tables.TableSchema;

class TableReaderBaseImpl<T> extends StreamReaderImpl<TRspReadTable> {
    private static final Parser<TRspReadTableMeta> META_PARSER = TRspReadTableMeta.parser();

    @Nullable protected TableAttachmentReader<T> reader;
    // Need for creating TableAttachmentReader later
    @Nullable private final Class<T> objectClazz;
    @Nullable private final ReadTable<T> req;
    protected TRspReadTableMeta metadata = null;

    TableReaderBaseImpl(ReadTable<T> req, Class<T> objectClazz) {
        this.req = req;
        this.objectClazz = objectClazz;
    }

    TableReaderBaseImpl(TableAttachmentReader<T> reader) {
        this.reader = reader;
        this.objectClazz = null;
        this.req = null;
    }

    @Override
    protected Parser<TRspReadTable> responseParser() {
        return TRspReadTable.parser();
    }

    public CompletableFuture<TableReaderBaseImpl<T>> waitMetadataImpl(SerializationResolver serializationResolver) {
        TableReaderBaseImpl<T> self = this;
        return readHead().thenApply((data) -> {
            self.metadata = RpcUtil.parseMessageBodyWithCompression(data, META_PARSER, Compression.None);
            if (self.reader == null) {
                Objects.requireNonNull(self.objectClazz);
                YTreeSerializer<T> serializer = serializationResolver.forClass(
                        self.objectClazz,
                        ApiServiceUtil.deserializeTableSchema(self.metadata.getSchema()));
                if (!(serializer.getClass().getName().equals(
                        "ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.YTreeObjectSerializer"))) {
                    throw new RuntimeException("Got not a YTreeObjectSerializer");
                }
                self.reader = new TableAttachmentWireProtocolReader<>(
                        MappedRowsetDeserializer.forClass((YTreeRowSerializer<T>) serializer));
            }

            return self;
        });
    }

    public boolean canRead() {
        return doCanRead();
    }

    public List<T> read() throws Exception {
        return reader.parse(doRead());
    }

    public CompletableFuture<Void> readyEvent() {
        return getReadyEvent();
    }
}

class TableReaderImpl<T> extends TableReaderBaseImpl<T> implements TableReader<T> {
    TableReaderImpl(ReadTable<T> req, Class<T> objectClazz) {
        super(req, objectClazz);
    }

    TableReaderImpl(TableAttachmentReader<T> reader) {
        super(reader);
    }

    @Override
    public long getStartRowIndex() {
        return metadata.getStartRowIndex();
    }

    @Override
    public long getTotalRowCount() {
        return reader.getTotalRowCount();
    }

    @Override
    public DataStatistics.TDataStatistics getDataStatistics() {
        return reader.getDataStatistics();
    }

    @Override
    public TableSchema getTableSchema() {
        return ApiServiceUtil.deserializeTableSchema(metadata.getSchema());
    }

    @Override
    public TableSchema getCurrentReadSchema() {
        final TableSchema schema = reader.getCurrentReadSchema();
        return schema != null ? schema : getTableSchema();
    }

    @Override
    public List<String> getOmittedInaccessibleColumns() {
        return metadata.getOmittedInaccessibleColumnsList();
    }

    public CompletableFuture<TableReader<T>> waitMetadata(SerializationResolver serializationResolver) {
        return waitMetadataImpl(serializationResolver).thenApply(reader -> (TableReader<T>) reader);
    }

    @Override
    public boolean canRead() {
        return super.canRead();
    }

    @Override
    public List<T> read() throws Exception {
        return super.read();
    }

    @Override
    public CompletableFuture<Void> readyEvent() {
        return super.readyEvent();
    }

    @Override
    public CompletableFuture<Void> close() {
        return doClose();
    }
}

class AsyncTableReaderImpl<T> extends TableReaderBaseImpl<T> implements AsyncReader<T> {

    AsyncTableReaderImpl(ReadTable<T> req, Class<T> objectClazz) {
        super(req, objectClazz);
    }

    AsyncTableReaderImpl(TableAttachmentReader<T> reader) {
        super(reader);
    }

    public CompletableFuture<AsyncReader<T>> waitMetadata(SerializationResolver serializationResolver) {
        return super.waitMetadataImpl(serializationResolver).thenApply(reader -> (AsyncReader<T>) reader);
    }

    @Override
    public CompletableFuture<Void> acceptAllAsync(Consumer<T> consumer, Executor executor) {
        return next().thenComposeAsync(rows -> {
            if (rows == null) {
                return CompletableFuture.completedFuture(null);
            }
            for (T row : rows) {
                consumer.accept(row);
            }
            return acceptAllAsync(consumer, executor);
        }, executor);
    }

    @Override
    public CompletableFuture<List<T>> next() {
        try {
            List<T> rows = read();
            if (rows != null) {
                return CompletableFuture.completedFuture(rows);
            }
            return readyEvent().thenCompose(unused -> {
                if (canRead()) {
                    return next();
                } else {
                    return CompletableFuture.completedFuture(null);
                }
            });
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void close() {
        control.cancel();
    }
}
