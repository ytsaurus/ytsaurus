package ru.yandex.yt.ytclient.proxy;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

import javax.annotation.Nullable;

import NYT.NChunkClient.NProto.DataStatistics;
import com.google.protobuf.Parser;

import ru.yandex.inside.yt.kosher.impl.ytree.object.YTreeSerializer;
import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.YTreeObjectSerializer;
import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.YTreeObjectSerializerFactory;
import ru.yandex.yt.rpcproxy.TRspReadTable;
import ru.yandex.yt.rpcproxy.TRspReadTableMeta;
import ru.yandex.yt.ytclient.object.MappedRowsetDeserializer;
import ru.yandex.yt.ytclient.proxy.internal.TableAttachmentReader;
import ru.yandex.yt.ytclient.proxy.internal.TableAttachmentWireProtocolReader;
import ru.yandex.yt.ytclient.rpc.RpcUtil;
import ru.yandex.yt.ytclient.rpc.internal.Compression;
import ru.yandex.yt.ytclient.tables.TableSchema;

class TableReaderImpl<T> extends StreamReaderImpl<TRspReadTable> implements TableReader<T> {
    private static final Parser<TRspReadTableMeta> META_PARSER = TRspReadTableMeta.parser();

    @Nullable private TableAttachmentReader<T> reader;
    // Need for creating TableAttachmentReader later
    @Nullable private final Class<T> objectClazz;
    private TRspReadTableMeta metadata = null;

    TableReaderImpl(Class<T> objectClazz) {
        this.objectClazz = objectClazz;
    }

    TableReaderImpl(TableAttachmentReader<T> reader) {
        this.reader = reader;
        this.objectClazz = null;
    }

    @Override
    protected Parser<TRspReadTable> responseParser() {
        return TRspReadTable.parser();
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

    public CompletableFuture<TableReader<T>> waitMetadata() {
        TableReaderImpl<T> self = this;
        return readHead().thenApply((data) -> {
            self.metadata = RpcUtil.parseMessageBodyWithCompression(data, META_PARSER, Compression.None);
            if (self.reader == null) {
                Objects.requireNonNull(self.objectClazz);
                YTreeSerializer<T> serializer = YTreeObjectSerializerFactory.forClass(
                        self.objectClazz, ApiServiceUtil.deserializeTableSchema(self.metadata.getSchema()));
                if (!(serializer instanceof YTreeObjectSerializer)) {
                    throw new RuntimeException("Got not a YTreeObjectSerializer");
                }
                self.reader = new TableAttachmentWireProtocolReader<>(
                        MappedRowsetDeserializer.forClass((YTreeObjectSerializer<T>) serializer));
            }

            return self;
        });
    }

    @Override
    public boolean canRead() {
        return doCanRead();
    }

    @Override
    public List<T> read() throws Exception {
        return reader.parse(doRead());
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
