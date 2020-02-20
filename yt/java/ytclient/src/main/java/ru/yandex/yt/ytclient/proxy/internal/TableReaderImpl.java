package ru.yandex.yt.ytclient.proxy.internal;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import NYT.NChunkClient.NProto.DataStatistics;

import ru.yandex.yt.rpcproxy.TReadTableMeta;
import ru.yandex.yt.rpcproxy.TRowsetDescriptor;
import ru.yandex.yt.rpcproxy.TRspReadTable;
import ru.yandex.yt.ytclient.proxy.ApiServiceUtil;
import ru.yandex.yt.ytclient.proxy.TableReader;
import ru.yandex.yt.ytclient.rpc.RpcClientStreamControl;
import ru.yandex.yt.ytclient.rpc.RpcMessageParser;
import ru.yandex.yt.ytclient.rpc.RpcUtil;
import ru.yandex.yt.ytclient.rpc.internal.Compression;
import ru.yandex.yt.ytclient.rpc.internal.RpcServiceMethodDescriptor;
import ru.yandex.yt.ytclient.tables.TableSchema;

public class TableReaderImpl<T> extends StreamReaderImpl<TRspReadTable> implements TableReader<T> {
    private static final RpcMessageParser<TReadTableMeta> metaParser =
            RpcServiceMethodDescriptor.makeMessageParser(TReadTableMeta.class);

    private final TableAttachmentReader<T> reader;
    private TReadTableMeta metadata = null;

    public TableReaderImpl(RpcClientStreamControl control, TableAttachmentReader<T> reader) {
        super(control);
        this.reader = reader;
    }

    @Override
    protected RpcMessageParser<TRspReadTable> responseParser() {
        return RpcServiceMethodDescriptor.makeMessageParser(TRspReadTable.class);
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

    @Override
    public TRowsetDescriptor getRowsetDescriptor() {
        return reader.getRowsetDescriptor();
    }

    public CompletableFuture<TableReader<T>> waitMetadata() {
        TableReaderImpl<T> self = this;
        return readHead().thenApply((data) -> {
            self.metadata = RpcUtil.parseMessageBodyWithCompression(data, metaParser, Compression.None);
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
