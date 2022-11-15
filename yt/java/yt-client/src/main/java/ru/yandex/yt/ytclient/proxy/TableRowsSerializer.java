package ru.yandex.yt.ytclient.proxy;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nullable;

import com.google.protobuf.CodedOutputStream;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import tech.ytsaurus.client.request.Format;

import ru.yandex.inside.yt.kosher.impl.ytree.object.YTreeSerializer;
import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;
import ru.yandex.yt.rpcproxy.ERowsetFormat;
import ru.yandex.yt.rpcproxy.TRowsetDescriptor;
import ru.yandex.yt.ytclient.SerializationResolver;
import ru.yandex.yt.ytclient.object.WireRowSerializer;
import ru.yandex.yt.ytclient.request.WriteTable;
import ru.yandex.yt.ytclient.serialization.YTreeBinarySerializer;
import ru.yandex.yt.ytclient.tables.ColumnSchema;
import ru.yandex.yt.ytclient.tables.ColumnValueType;
import ru.yandex.yt.ytclient.tables.TableSchema;
import ru.yandex.yt.ytclient.wire.UnversionedRow;
import ru.yandex.yt.ytclient.wire.UnversionedValue;
import ru.yandex.yt.ytclient.wire.WireProtocolWriter;

@NonNullApi
@NonNullFields
class TableRowsWireSerializer<T> extends TableRowsSerializer<T> {
    private final WireRowSerializer<T> wireRowSerializer;

    TableRowsWireSerializer(WireRowSerializer<T> wireRowSerializer) {
        super(ERowsetFormat.RF_YT_WIRE);
        this.wireRowSerializer = Objects.requireNonNull(wireRowSerializer);
    }

    @Override
    public TableSchema getSchema() {
        return wireRowSerializer.getSchema();
    }

    @Override
    protected void writeMergedRow(
            ByteBuf buf,
            TRowsetDescriptor descriptor,
            List<T> rows,
            int[] idMapping
    ) {
        WireProtocolWriter writer = new WireProtocolWriter();
        wireRowSerializer.updateSchema(descriptor);
        writer.writeUnversionedRowset(rows, wireRowSerializer, idMapping);

        for (byte[] bytes : writer.finish()) {
            buf.writeBytes(bytes);
        }
    }

    @Override
    protected void writeMergedRowWithoutCount(
            ByteBuf buf, TRowsetDescriptor descriptor, List<T> rows, int[] idMapping) {
        WireProtocolWriter writer = new WireProtocolWriter();
        wireRowSerializer.updateSchema(descriptor);
        writer.writeUnversionedRowsetWithoutCount(rows, wireRowSerializer, idMapping);

        for (byte[] bytes : writer.finish()) {
            buf.writeBytes(bytes);
        }
    }

    public void writeMeta(ByteBuf buf, ByteBuf serializedRows, int rowsCount) {
        int mergedRowSizeIndex = buf.writerIndex();
        buf.writeLongLE(0); // reserve space

        // Write rows count
        WireProtocolWriter writer = new WireProtocolWriter();
        writer.writeRowCount(rowsCount);
        for (byte[] bytes : writer.finish()) {
            buf.writeBytes(bytes);
        }

        // Save serialized rows data's size
        buf.setLongLE(mergedRowSizeIndex,
                serializedRows.readableBytes() + (buf.writerIndex() - mergedRowSizeIndex) - 8);
    }
}

@NonNullApi
@NonNullFields
class TableRowsYsonSerializer<T> extends TableRowsSerializer<T> {
    private final Format format;
    private final YTreeSerializer<T> ysonSerializer;
    private TableSchema schema;

    TableRowsYsonSerializer(Format format, YTreeSerializer<T> ysonSerializer, TableSchema schema) {
        super(ERowsetFormat.RF_FORMAT);
        this.format = format;
        this.ysonSerializer = ysonSerializer;
        this.schema = schema;
    }

    @Override
    public TableSchema getSchema() {
        return schema;
    }

    @Override
    protected void writeMergedRow(
            ByteBuf buf,
            TRowsetDescriptor descriptor,
            List<T> rows,
            int[] idMapping
    ) {
        updateSchema(descriptor);
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        YTreeBinarySerializer.serializeAllObjects(rows, ysonSerializer, output);
        buf.writeBytes(output.toByteArray());
    }

    @Override
    protected void writeMergedRowWithoutCount(
            ByteBuf buf, TRowsetDescriptor descriptor, List<T> rows, int[] idMapping) {
        writeMergedRow(buf, descriptor, rows, idMapping);
    }

    @Override
    public void writeMeta(ByteBuf buf, ByteBuf serializedRows, int rowsCount) {
    }

    private void updateSchema(TRowsetDescriptor schemaDelta) {
        TableSchema.Builder builder = this.schema.toBuilder();
        for (TRowsetDescriptor.TNameTableEntry entry : schemaDelta.getNameTableEntriesList()) {
            if (schema.findColumn(entry.getName()) != -1) {
                builder.add(new ColumnSchema(entry.getName(), ColumnValueType.fromValue(entry.getType())));
            }
        }
        this.schema = builder.build();
    }
}

@NonNullApi
@NonNullFields
abstract class TableRowsSerializer<T> {
    private static final String YSON = "yson";

    private TRowsetDescriptor rowsetDescriptor;
    private final Map<String, Integer> column2id = new HashMap<>();
    private final ERowsetFormat rowsetFormat;

    TableRowsSerializer(ERowsetFormat rowsetFormat) {
        this.rowsetFormat = rowsetFormat;
        this.rowsetDescriptor = TRowsetDescriptor.newBuilder().setRowsetFormat(rowsetFormat).build();
    }

    public abstract TableSchema getSchema();

    @Nullable
    public static <T> TableRowsSerializer<T> createTableRowsSerializer(
            WriteTable.SerializationContext<T> context,
            SerializationResolver serializationResolver
    ) {
        if (context.getRowsetFormat() == ERowsetFormat.RF_YT_WIRE) {
            Optional<WireRowSerializer<T>> reqSerializer = context.getSerializer();
            if (reqSerializer.isPresent()) {
                return new TableRowsWireSerializer<>(reqSerializer.get());
            }

            Optional<YTreeSerializer<T>> ysonSerializer = context.getYsonSerializer();
            if (ysonSerializer.isPresent()) {
                return new TableRowsWireSerializer<>(
                        serializationResolver.createWireRowSerializer(ysonSerializer.get()));
            }
            return null;
        } else if (context.getRowsetFormat() == ERowsetFormat.RF_FORMAT) {
            if (!context.getFormat().isPresent()) {
                throw new IllegalArgumentException("No format with RF_FORMAT");
            }
            if (!context.getYsonSerializer().isPresent()) {
                throw new IllegalArgumentException("No yson serializer for RF_FORMAT");
            }
            if (!context.getFormat().get().getType().equals(YSON)) {
                throw new IllegalArgumentException(
                        "Format " + context.getFormat().get().getType() + " isn't supported");
            }
            YTreeSerializer<T> serializer = context.getYsonSerializer().get();
            return new TableRowsYsonSerializer<>(
                    context.getFormat().get(),
                    serializer,
                    serializationResolver.asTableSchema(serializer));
        } else {
            throw new IllegalArgumentException("Unsupported rowset format");
        }
    }

    public ByteBuf serializeRows(List<T> rows, TableSchema schema) {
        TRowsetDescriptor currentDescriptor = getCurrentRowsetDescriptor(schema);
        int[] idMapping = getIdMapping(rows, schema);

        ByteBuf buf = Unpooled.buffer();
        writeMergedRowWithoutCount(buf, currentDescriptor, rows, idMapping);

        updateRowsetDescriptor(currentDescriptor);

        return buf;
    }

    public byte[] serialize(ByteBuf serializedRows, int rowsCount) throws IOException {
        ByteBuf buf = Unpooled.buffer();

        // parts
        buf.writeIntLE(2);

        int descriptorSizeIndex = buf.writerIndex();
        buf.writeLongLE(0); // reserve space

        writeDescriptor(buf, rowsetDescriptor);

        buf.setLongLE(descriptorSizeIndex, buf.writerIndex() - descriptorSizeIndex - 8);

        writeMeta(buf, serializedRows, rowsCount);

        int bufSize = buf.readableBytes();

        // Convert to array
        byte[] result = new byte[bufSize + serializedRows.readableBytes()];
        buf.readBytes(result, 0, bufSize);
        if (buf.readableBytes() != 0) {
            throw new IllegalStateException();
        }

        // Write serialized rows data
        serializedRows.readBytes(result, bufSize, serializedRows.readableBytes());
        if (serializedRows.readableBytes() != 0) {
            throw new IllegalStateException();
        }

        return result;
    }

    public abstract void writeMeta(ByteBuf buf, ByteBuf serializedRows, int rowsCount);

    public byte[] serialize(List<T> rows, TableSchema schema) throws IOException {
        TRowsetDescriptor currentDescriptor = getCurrentRowsetDescriptor(schema);
        int[] idMapping = getIdMapping(rows, schema);

        ByteBuf buf = Unpooled.buffer();
        writeRowsData(buf, currentDescriptor, rows, idMapping);

        updateRowsetDescriptor(currentDescriptor);

        return bufToArray(buf);
    }

    private TRowsetDescriptor getCurrentRowsetDescriptor(TableSchema schema) {
        TRowsetDescriptor.Builder builder = TRowsetDescriptor.newBuilder();

        for (ColumnSchema descriptor : schema.getColumns()) {
            if (!column2id.containsKey(descriptor.getName())) {
                builder.addNameTableEntries(TRowsetDescriptor.TNameTableEntry.newBuilder()
                        .setName(descriptor.getName())
                        .setType(descriptor.getType().getValue())
                        .build());

                column2id.put(descriptor.getName(), column2id.size());
            }
        }
        builder.setRowsetFormat(rowsetFormat);

        return builder.build();
    }

    private int[] getIdMapping(List<T> rows, TableSchema schema) {
        Iterator<T> it = rows.iterator();
        if (!it.hasNext()) {
            throw new IllegalStateException();
        }

        T first = it.next();
        boolean isUnversionedRows = first instanceof List && ((List<?>) first).get(0) instanceof UnversionedRow;

        int[] idMapping = isUnversionedRows
                ? new int[column2id.size()]
                : null;

        if (isUnversionedRows) {
            for (UnversionedRow row : (List<UnversionedRow>) rows) {
                List<UnversionedValue> values = row.getValues();
                for (int columnNumber = 0;
                     columnNumber < schema.getColumns().size() && columnNumber < values.size();
                     ++columnNumber
                ) {
                    String columnName = schema.getColumnName(columnNumber);
                    UnversionedValue value = values.get(columnNumber);
                    int columnId = column2id.get(columnName);
                    idMapping[value.getId()] = columnId;
                }
            }
        }

        return idMapping;
    }

    private void updateRowsetDescriptor(TRowsetDescriptor currentDescriptor) {
        if (currentDescriptor.getNameTableEntriesCount() > 0) {
            TRowsetDescriptor.Builder merged = TRowsetDescriptor.newBuilder();
            merged.setRowsetFormat(rowsetFormat);
            merged.mergeFrom(rowsetDescriptor);
            merged.addAllNameTableEntries(currentDescriptor.getNameTableEntriesList());
            rowsetDescriptor = merged.build();
        }
    }

    private byte[] bufToArray(ByteBuf buf) {
        byte[] attachment = new byte[buf.readableBytes()];
        buf.readBytes(attachment, 0, attachment.length);

        if (buf.readableBytes() != 0) {
            throw new IllegalStateException();
        }

        return attachment;
    }

    private void writeDescriptor(ByteBuf buf, TRowsetDescriptor descriptor) throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        CodedOutputStream os = CodedOutputStream.newInstance(byteArrayOutputStream);
        descriptor.writeTo(os);
        os.flush();

        buf.writeBytes(byteArrayOutputStream.toByteArray());
    }

    protected abstract void writeMergedRowWithoutCount(
            ByteBuf buf, TRowsetDescriptor descriptor, List<T> rows, int[] idMapping);

    private void writeRowsData(
            ByteBuf buf,
            TRowsetDescriptor descriptorDelta,
            List<T> rows,
            int[] idMapping
    ) throws IOException {
        // parts
        buf.writeIntLE(2);

        int descriptorDeltaSizeIndex = buf.writerIndex();
        buf.writeLongLE(0);  // reserve space

        writeDescriptor(buf, descriptorDelta);

        buf.setLongLE(descriptorDeltaSizeIndex, buf.writerIndex() - descriptorDeltaSizeIndex - 8);

        int mergedRowSizeIndex = buf.writerIndex();
        buf.writeLongLE(0);  // reserve space

        writeMergedRow(buf, descriptorDelta, rows, idMapping);

        buf.setLongLE(mergedRowSizeIndex, buf.writerIndex() - mergedRowSizeIndex - 8);
    }

    protected abstract void writeMergedRow(
            ByteBuf buf,
            TRowsetDescriptor descriptor,
            List<T> rows,
            int[] idMapping);
}
