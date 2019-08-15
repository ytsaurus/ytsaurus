package ru.yandex.yt.ytclient.proxy.internal;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

import com.google.protobuf.CodedOutputStream;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import ru.yandex.bolts.collection.Cf;
import ru.yandex.bolts.collection.MapF;
import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.YTreeObjectSerializer;
import ru.yandex.yt.rpcproxy.TRowsetDescriptor;
import ru.yandex.yt.rpcproxy.TRspWriteTable;
import ru.yandex.yt.rpcproxy.TWriteTableMeta;
import ru.yandex.yt.ytclient.object.MappedRowSerializer;
import ru.yandex.yt.ytclient.object.WireRowSerializer;
import ru.yandex.yt.ytclient.proxy.ApiServiceUtil;
import ru.yandex.yt.ytclient.proxy.TableWriter;
import ru.yandex.yt.ytclient.rpc.RpcClientStreamControl;
import ru.yandex.yt.ytclient.rpc.RpcMessageParser;
import ru.yandex.yt.ytclient.rpc.RpcStreamConsumer;
import ru.yandex.yt.ytclient.rpc.RpcUtil;
import ru.yandex.yt.ytclient.rpc.internal.Compression;
import ru.yandex.yt.ytclient.rpc.internal.RpcServiceMethodDescriptor;
import ru.yandex.yt.ytclient.tables.ColumnSchema;
import ru.yandex.yt.ytclient.tables.TableSchema;
import ru.yandex.yt.ytclient.wire.UnversionedRow;
import ru.yandex.yt.ytclient.wire.UnversionedValue;
import ru.yandex.yt.ytclient.wire.WireProtocolWriter;

public class TableWriterImpl<T> extends StreamWriterImpl<TRspWriteTable> implements TableWriter<T>, RpcStreamConsumer {
    private TableSchema schema;
    private TRowsetDescriptor rowsetDescriptor = TRowsetDescriptor.newBuilder().build();
    private final WireRowSerializer<T> serializer;

    public TableWriterImpl(RpcClientStreamControl control, long windowSize, long packetSize, YTreeObjectSerializer<T> serializer) {
        this(control, windowSize, packetSize, Objects.requireNonNull(MappedRowSerializer.forClass(serializer)));
    }

    public TableWriterImpl(RpcClientStreamControl control, long windowSize, long packetSize, WireRowSerializer<T> serializer) {
        super(control, windowSize, packetSize);

        this.serializer = Objects.requireNonNull(serializer);
    }

    public WireRowSerializer<T> getRowSerializer() {
        return this.serializer;
    }

    @Override
    protected RpcMessageParser<TRspWriteTable> responseParser() {
        return RpcServiceMethodDescriptor.makeMessageParser(TRspWriteTable.class);
    }

    public CompletableFuture<TableWriter> startUpload() {
        TableWriterImpl self = this;

        return startUpload.thenApply((attachments) -> {
            if (attachments.size() != 1) {
                throw new IllegalArgumentException("protocol error");
            }
            byte[] head = attachments.get(0);
            if (head == null) {
                throw new IllegalArgumentException("protocol error");
            }

            RpcMessageParser<TWriteTableMeta> metaParser = RpcServiceMethodDescriptor.makeMessageParser(TWriteTableMeta.class);
            TWriteTableMeta metadata = RpcUtil.parseMessageBodyWithCompression(head, metaParser, Compression.None);

            self.schema = ApiServiceUtil.deserializeTableSchema(metadata.getSchema());

            logger.debug("schema -> {}", schema.toYTree().toString());

            return self;
        });
    }

    private void writeDescriptorDelta(ByteBuf buf, TRowsetDescriptor descriptor) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        CodedOutputStream os = CodedOutputStream.newInstance(baos);
        descriptor.writeTo(os);
        os.flush();

        buf.writeBytes(baos.toByteArray());
    }

    private void writeMergedRow(ByteBuf buf, List<T> rows, int[] idMapping) {
        WireProtocolWriter writer = new WireProtocolWriter();
        writer.writeUnversionedRowset(rows, serializer, idMapping);

        for (byte [] bytes : writer.finish()) {
            buf.writeBytes(bytes);
        }
    }

    private void writeRowsdata(ByteBuf buf, TRowsetDescriptor descriptor, List<T> rows, int[] idMapping) throws IOException {
        // parts
        buf.writeIntLE(2);

        int descriptorDeltaSizeIndex = buf.writerIndex();
        buf.writeLongLE(0); // reserve space

        writeDescriptorDelta(buf, descriptor);

        buf.setLongLE(descriptorDeltaSizeIndex, buf.writerIndex() - descriptorDeltaSizeIndex - 8);

        int mergedRowSizeIndex = buf.writerIndex();
        buf.writeLongLE(0); // reserve space

        writeMergedRow(buf, rows, idMapping);

        buf.setLongLE(mergedRowSizeIndex, buf.writerIndex() - mergedRowSizeIndex - 8);
    }

    private final MapF<String, Integer> column2id = Cf.hashMap();

    @Override
    public boolean write(List<T> rows, TableSchema schema) throws IOException {
        Iterator<T> it = rows.iterator();
        if (!it.hasNext()) {
            throw new IllegalStateException();
        }

        T first = it.next();
        boolean isUnversionedRows = first instanceof List && ((List) first).get(0) instanceof UnversionedRow;

        TRowsetDescriptor.Builder builder = TRowsetDescriptor.newBuilder();

        for (ColumnSchema descriptor : schema.getColumns()) {
            if (!column2id.containsKey(descriptor.getName())) {
                builder.addColumns(TRowsetDescriptor.TColumnDescriptor.newBuilder()
                        .setName(descriptor.getName())
                        .setType(descriptor.getType().getValue())
                        .build());

                column2id.put(descriptor.getName(), column2id.size());
            }
        }

        ByteBuf buf = Unpooled.buffer();

        TRowsetDescriptor currentDescriptor = builder.build();

        int[] idMapping = isUnversionedRows
            ? new int[column2id.size()]
            : null;

        if (isUnversionedRows) {
            for (UnversionedRow row : (List<UnversionedRow>)rows) {
                List<UnversionedValue> values = row.getValues();
                for (int columnNumber = 0; columnNumber < schema.getColumns().size() && columnNumber < values.size(); ++columnNumber) {
                    String columnName = schema.getColumnName(columnNumber);
                    UnversionedValue value = values.get(columnNumber);
                    int columnId = column2id.get(columnName);
                    idMapping[value.getId()] = columnId;
                }
            }
        }

        writeRowsdata(buf, currentDescriptor, rows, idMapping);

        byte[] attachment = new byte[buf.readableBytes()];
        buf.readBytes(attachment, 0, attachment.length);

        if (buf.readableBytes() != 0) {
            throw new IllegalStateException();
        }

        if (currentDescriptor.getColumnsCount() > 0) {
            TRowsetDescriptor.Builder merged = TRowsetDescriptor.newBuilder();
            merged.mergeFrom(rowsetDescriptor);
            merged.addAllColumns(currentDescriptor.getColumnsList());
            rowsetDescriptor = merged.build();
        }

        return push(attachment);
    }

    @Override
    public TRowsetDescriptor getRowsetDescriptor() {
        return rowsetDescriptor;
    }

    @Override
    public TableSchema getTableSchema() {
        return schema;
    }
}
