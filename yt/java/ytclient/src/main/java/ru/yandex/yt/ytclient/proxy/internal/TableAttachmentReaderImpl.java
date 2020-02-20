package ru.yandex.yt.ytclient.proxy.internal;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.Nullable;

import NYT.NChunkClient.NProto.DataStatistics.TDataStatistics;

import ru.yandex.bolts.collection.Cf;
import ru.yandex.yt.rpcproxy.TRowsetDescriptor;
import ru.yandex.yt.rpcproxy.TTableReaderPayload;
import ru.yandex.yt.ytclient.object.WireRowDeserializer;
import ru.yandex.yt.ytclient.proxy.ApiServiceUtil;
import ru.yandex.yt.ytclient.tables.TableSchema;
import ru.yandex.yt.ytclient.wire.WireProtocolReader;

public class TableAttachmentReaderImpl<T> implements TableAttachmentReader<T> {
    private final WireRowDeserializer<T> deserializer;
    private final AtomicLong totalRowCount = new AtomicLong(-1);
    private volatile TDataStatistics currentDataStatistics;

    private volatile TRowsetDescriptor currentRowsetDescriptor;
    private volatile TableSchema currentReadSchema;

    public TableAttachmentReaderImpl(WireRowDeserializer<T> deserializer) {
        this.deserializer = Objects.requireNonNull(deserializer);
    }

    private void parseDescriptorDelta(ByteBuffer bb, int size) throws Exception {
        int endPosition = bb.position() + size;
        TRowsetDescriptor rowsetDescriptor = TRowsetDescriptor.parseFrom((ByteBuffer) bb.slice().limit(size)); //
        // (ByteBuffer) for java8 compatibility
        ApiServiceUtil.validateRowsetDescriptor(rowsetDescriptor);

        if (currentReadSchema == null) {
            currentRowsetDescriptor = rowsetDescriptor;
            currentReadSchema = ApiServiceUtil.deserializeRowsetSchema(rowsetDescriptor);
        } else if (rowsetDescriptor.getColumnsCount() > 0) {
            TRowsetDescriptor.Builder builder = TRowsetDescriptor.newBuilder();
            builder.mergeFrom(currentRowsetDescriptor);
            builder.addAllColumns(rowsetDescriptor.getColumnsList());
            currentRowsetDescriptor = builder.build();
            currentReadSchema = ApiServiceUtil.deserializeRowsetSchema(currentRowsetDescriptor);
        }

        bb.position(endPosition);
    }

    private List<T> parseMergedRow(ByteBuffer bb, int size) {
        byte[] data = new byte[size];
        bb.get(data);

        WireProtocolReader reader = new WireProtocolReader(Cf.list(data));

        deserializer.updateSchema(currentReadSchema);

        int rowCount = reader.readRowCount();

        List<T> rows = new ArrayList<>(rowCount);

        for (int i = 0; i < rowCount; ++i) {
            rows.add(reader.readUnversionedRow(deserializer));
        }

        return rows;
    }

    private List<T> parseRowData(ByteBuffer bb, int size) throws Exception {
        int endPosition = bb.position() + size;

        int parts = bb.getInt();

        if (parts != 2) {
            throw new IllegalArgumentException();
        }

        int descriptorDeltaSize = (int) bb.getLong();
        parseDescriptorDelta(bb, descriptorDeltaSize);

        int mergedRowSize = (int) bb.getLong();
        List<T> rowset = parseMergedRow(bb, mergedRowSize);

        if (bb.position() != endPosition) {
            throw new IllegalArgumentException();
        }

        return rowset;
    }

    private void parsePayload(ByteBuffer bb, int size) throws Exception {
        int endPosition = bb.position() + size;
        TTableReaderPayload payload = TTableReaderPayload.parseFrom((ByteBuffer) bb.slice().limit(size)); //
        // (ByteBuffer) for java8 compatibility
        currentDataStatistics = payload.getDataStatistics();
        totalRowCount.set(payload.getTotalRowCount());
        bb.position(endPosition);
    }

    private List<T> parseRowsWithPayload(byte[] attachment) throws Exception {
        if (attachment == null) {
            return null;
        }

        ByteBuffer bb = ByteBuffer.wrap(attachment).order(ByteOrder.LITTLE_ENDIAN);
        int parts = bb.getInt();
        if (parts != 2) {
            throw new IllegalArgumentException();
        }

        int rowDataSize = (int) bb.getLong();

        List<T> rowset = parseRowData(bb, rowDataSize);

        int payloadSize = (int) bb.getLong();

        parsePayload(bb, payloadSize);

        if (bb.hasRemaining()) {
            throw new IllegalArgumentException();
        }

        return rowset;
    }

    @Override
    public List<T> parse(byte[] attachments) throws Exception {
        return parseRowsWithPayload(attachments);
    }

    @Override
    public long getTotalRowCount() {
        return totalRowCount.get();
    }

    @Override
    public TDataStatistics getDataStatistics() {
        return currentDataStatistics;
    }

    @Nullable
    @Override
    public TableSchema getCurrentReadSchema() {
        return currentReadSchema;
    }

    @Nullable
    @Override
    public TRowsetDescriptor getRowsetDescriptor() {
        return currentRowsetDescriptor;
    }
}
