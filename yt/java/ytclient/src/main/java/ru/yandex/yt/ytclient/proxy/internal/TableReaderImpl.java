package ru.yandex.yt.ytclient.proxy.internal;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import NYT.NChunkClient.NProto.DataStatistics;

import ru.yandex.bolts.collection.Cf;
import ru.yandex.misc.ExceptionUtils;
import ru.yandex.yt.rpcproxy.TReadTableMeta;
import ru.yandex.yt.rpcproxy.TRowsetDescriptor;
import ru.yandex.yt.rpcproxy.TRspReadTable;
import ru.yandex.yt.rpcproxy.TTableReaderPayload;
import ru.yandex.yt.ytclient.object.UnversionedRowsetDeserializer;
import ru.yandex.yt.ytclient.proxy.ApiServiceUtil;
import ru.yandex.yt.ytclient.proxy.TableReader;
import ru.yandex.yt.ytclient.rpc.RpcClientStreamControl;
import ru.yandex.yt.ytclient.rpc.RpcMessageParser;
import ru.yandex.yt.ytclient.rpc.RpcUtil;
import ru.yandex.yt.ytclient.rpc.internal.RpcServiceMethodDescriptor;
import ru.yandex.yt.ytclient.tables.TableSchema;
import ru.yandex.yt.ytclient.wire.UnversionedRowset;
import ru.yandex.yt.ytclient.wire.WireProtocolReader;

public class TableReaderImpl extends StreamReaderImpl<TRspReadTable> implements TableReader {
    private TReadTableMeta metadata = null;
    private final static RpcMessageParser<TReadTableMeta> metaParser = RpcServiceMethodDescriptor.makeMessageParser(TReadTableMeta.class);

    final private Object lock = new Object();
    private TRowsetDescriptor currentRowsetDescriptor = null;
    private TableSchema currentReadSchema = null;
    private UnversionedRowsetDeserializer deserializer = null;
    private DataStatistics.TDataStatistics currentDataStatistics = null;
    private long totalRowCount = -1;

    public TableReaderImpl(RpcClientStreamControl control) {
        super(control);
    }

    @Override
    protected RpcMessageParser<TRspReadTable> responseParser() {
        return RpcServiceMethodDescriptor.makeMessageParser(TRspReadTable.class);
    }

    private void parseDescriptorDeltaRefSize(ByteBuffer bb, int size) throws Exception {
        int endPosition = bb.position() + size;
        TRowsetDescriptor rowsetDescriptor = TRowsetDescriptor.parseFrom((ByteBuffer)bb.slice().limit(size)); // (ByteBuffer) for java8 compatibility
        ApiServiceUtil.validateRowsetDescriptor(rowsetDescriptor);

        if (currentReadSchema == null) {
            currentReadSchema = ApiServiceUtil.deserializeRowsetSchema(rowsetDescriptor);
            currentRowsetDescriptor = rowsetDescriptor;
            deserializer = new UnversionedRowsetDeserializer(currentReadSchema);
        } else if (rowsetDescriptor.getColumnsCount() > 0) {
            TRowsetDescriptor.Builder builder = TRowsetDescriptor.newBuilder();
            builder.mergeFrom(currentRowsetDescriptor);
            builder.addAllColumns(rowsetDescriptor.getColumnsList());
            currentRowsetDescriptor = builder.build();
            currentReadSchema = ApiServiceUtil.deserializeRowsetSchema(currentRowsetDescriptor);
            deserializer = new UnversionedRowsetDeserializer(currentReadSchema);
        }

        logger.debug("{}", rowsetDescriptor);
        bb.position(endPosition);
    }

    private UnversionedRowset parseMergedRowRefs(ByteBuffer bb, int size) {
        byte[] data = new byte[size];
        bb.get(data);
        return new WireProtocolReader(Cf.list(data)).readUnversionedRowset(deserializer).getRowset();
    }

    private UnversionedRowset parseRowData(ByteBuffer bb, int size) throws Exception {
        int endPosition = bb.position() + size;

        int parts = bb.getInt();

        if (parts != 2) {
            throw new IllegalArgumentException();
        }

        int descriptorDeltaRefSize = (int)bb.getLong();
        parseDescriptorDeltaRefSize(bb, descriptorDeltaRefSize);

        int mergedRowRefsSize = (int)bb.getLong();
        UnversionedRowset rowset = parseMergedRowRefs(bb, mergedRowRefsSize);

        if (bb.position() != endPosition) {
            throw new IllegalArgumentException();
        }

        return rowset;
    }

    private void parsePayload(ByteBuffer bb, int size) throws Exception {
        int endPosition = bb.position() + size;
        TTableReaderPayload payload = TTableReaderPayload.parseFrom((ByteBuffer)bb.slice().limit(size)); // (ByteBuffer) for java8 compatibility
        synchronized (lock) {
            currentDataStatistics = payload.getDataStatistics();
            totalRowCount = payload.getTotalRowCount();
        }
        bb.position(endPosition);
    }

    private UnversionedRowset parseRowsWithPayload(byte[] attachment) throws Exception {
        if (attachment == null) {
            return null;
        }

        ByteBuffer bb = ByteBuffer.wrap(attachment).order(ByteOrder.LITTLE_ENDIAN);
        int parts = bb.getInt();
        if (parts != 2) {
            throw new IllegalArgumentException();
        }

        int rowDataSize = (int)bb.getLong();

        UnversionedRowset rowset = parseRowData(bb, rowDataSize);

        int payloadSize = (int)bb.getLong();

        parsePayload(bb, payloadSize);

        if (bb.hasRemaining()) {
            throw new IllegalArgumentException();
        }

        return rowset;
    }

    @Override
    public long getStartRowIndex() {
        return metadata.getStartRowIndex();
    }

    @Override
    public long getTotalRowCount() {
        synchronized (lock) {
            return totalRowCount;
        }
    }

    @Override
    public DataStatistics.TDataStatistics getDataStatistics() {
        synchronized (lock) {
            return currentDataStatistics;
        }
    }

    @Override
    public TableSchema getTableSchema() {
        return ApiServiceUtil.deserializeTableSchema(metadata.getSchema());
    }

    @Override
    public TRowsetDescriptor getRowsetDescriptor() {
        synchronized (lock) {
            return currentRowsetDescriptor;
        }
    }

    public CompletableFuture<TableReader> waitMetadata() {
        TableReaderImpl self = this;
        return readHead().thenApply((data) -> {
            self.metadata = RpcUtil.parseMessageBodyWithCompression(data, metaParser, compression);
            return self;
        });
    }

    @Override
    public UnversionedRowset read() throws Exception {
        return parseRowsWithPayload(doRead());
    }

    @Override
    public CompletableFuture<Void> read(Consumer<UnversionedRowset> consumer) {
        return doRead((next) -> {
            try {
                consumer.accept(parseRowsWithPayload(next));
            } catch (Exception ex) {
                throw ExceptionUtils.translate(ex);
            }

            return true;
        });
    }
}
