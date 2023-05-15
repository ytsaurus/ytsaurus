package tech.ytsaurus.client;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.Message;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import tech.ytsaurus.client.request.SerializationContext;
import tech.ytsaurus.client.rows.EntitySkiffSerializer;
import tech.ytsaurus.client.rows.UnversionedRow;
import tech.ytsaurus.client.rows.UnversionedValue;
import tech.ytsaurus.client.rows.WireProtocolWriter;
import tech.ytsaurus.client.rows.WireRowSerializer;
import tech.ytsaurus.core.operations.YTreeBinarySerializer;
import tech.ytsaurus.core.rows.YTreeSerializer;
import tech.ytsaurus.core.tables.ColumnSchema;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.lang.NonNullApi;
import tech.ytsaurus.lang.NonNullFields;
import tech.ytsaurus.rpcproxy.ERowsetFormat;
import tech.ytsaurus.rpcproxy.TRowsetDescriptor;

import static tech.ytsaurus.core.utils.ClassUtils.castToType;


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
    protected void writeRows(
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
    protected void writeRowsWithoutCount(
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
    private final YTreeSerializer<T> ysonSerializer;

    TableRowsYsonSerializer(YTreeSerializer<T> ysonSerializer) {
        super(ERowsetFormat.RF_FORMAT);
        this.ysonSerializer = ysonSerializer;
    }

    @Override
    protected void writeRows(
            ByteBuf buf,
            TRowsetDescriptor descriptor,
            List<T> rows,
            int[] idMapping
    ) {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        YTreeBinarySerializer.serializeAllObjects(rows, ysonSerializer, output);
        buf.writeBytes(output.toByteArray());
    }

    @Override
    protected void writeRowsWithoutCount(
            ByteBuf buf, TRowsetDescriptor descriptor, List<T> rows, int[] idMapping) {
        writeRows(buf, descriptor, rows, idMapping);
    }

    @Override
    public void writeMeta(ByteBuf buf, ByteBuf serializedRows, int rowsCount) {
        buf.writeLongLE(serializedRows.readableBytes());
    }

    @Override
    protected TRowsetDescriptor getCurrentRowsetDescriptor(TableSchema schema) {
        return rowsetDescriptor;
    }
}

@NonNullApi
@NonNullFields
class TableRowsSkiffSerializer<T> extends TableRowsSerializer<T> {
    private final EntitySkiffSerializer<T> serializer;

    TableRowsSkiffSerializer(EntitySkiffSerializer<T> serializer) {
        super(ERowsetFormat.RF_FORMAT);
        this.serializer = serializer;
    }

    @Override
    protected void writeRows(
            ByteBuf buf,
            TRowsetDescriptor descriptor,
            List<T> rows,
            int[] idMapping
    ) {
        rows.forEach(row -> {
            buf.writeByte(0);
            buf.writeByte(0);
            buf.writeBytes(serializer.serialize(row));
        });
    }

    @Override
    protected void writeRowsWithoutCount(
            ByteBuf buf, TRowsetDescriptor descriptor, List<T> rows, int[] idMapping) {
        writeRows(buf, descriptor, rows, idMapping);
    }

    @Override
    public void writeMeta(ByteBuf buf, ByteBuf serializedRows, int rowsCount) {
        buf.writeLongLE(serializedRows.readableBytes());
    }

    @Override
    protected TRowsetDescriptor getCurrentRowsetDescriptor(TableSchema schema) {
        return rowsetDescriptor;
    }
}

@NonNullApi
@NonNullFields
class TableRowsProtobufSerializer<T extends Message> extends TableRowsSerializer<T> {
    TableRowsProtobufSerializer() {
        super(ERowsetFormat.RF_FORMAT);
    }

    @Override
    protected void writeRows(
            ByteBuf buf,
            TRowsetDescriptor descriptor,
            List<T> rows,
            int[] idMapping
    ) {
        rows.forEach(row -> {
            byte[] messageBytes = row.toByteArray();
            buf.writeIntLE(messageBytes.length);
            buf.writeBytes(messageBytes);
        });
    }

    @Override
    protected void writeRowsWithoutCount(
            ByteBuf buf, TRowsetDescriptor descriptor, List<T> rows, int[] idMapping) {
        writeRows(buf, descriptor, rows, idMapping);
    }

    @Override
    public void writeMeta(ByteBuf buf, ByteBuf serializedRows, int rowsCount) {
        buf.writeLongLE(serializedRows.readableBytes());
    }

    @Override
    protected TRowsetDescriptor getCurrentRowsetDescriptor(TableSchema schema) {
        return rowsetDescriptor;
    }
}

@NonNullApi
@NonNullFields
abstract class TableRowsSerializer<T> {
    private static final String YSON = "yson";
    protected TRowsetDescriptor rowsetDescriptor;
    private final Map<String, Integer> columnToId = new HashMap<>();
    private final ERowsetFormat rowsetFormat;

    TableRowsSerializer(ERowsetFormat rowsetFormat) {
        this.rowsetFormat = rowsetFormat;
        this.rowsetDescriptor = TRowsetDescriptor.newBuilder().setRowsetFormat(rowsetFormat).build();
    }

    public TableSchema getSchema() {
        return TableSchema.builder().build();
    }

    static <T> Optional<TableRowsSerializer<T>> createTableRowsSerializer(
            SerializationContext<T> context,
            SerializationResolver serializationResolver
    ) {
        if (context.getRowsetFormat() == ERowsetFormat.RF_YT_WIRE) {
            Optional<WireRowSerializer<T>> reqSerializer = context.getWireSerializer();
            if (reqSerializer.isPresent()) {
                return Optional.of(new TableRowsWireSerializer<>(reqSerializer.get()));
            }

            Optional<YTreeSerializer<T>> ysonSerializer = context.getYtreeSerializer();
            if (ysonSerializer.isPresent()) {
                return Optional.of(new TableRowsWireSerializer<>(
                        serializationResolver.createWireRowSerializer(ysonSerializer.get())));
            }
            return Optional.empty();
        } else if (context.getRowsetFormat() == ERowsetFormat.RF_FORMAT) {
            if (context.getFormat().isEmpty()) {
                throw new IllegalArgumentException("No format with RF_FORMAT");
            }
            if (context.getSkiffSerializer().isPresent()) {
                return Optional.of(new TableRowsSkiffSerializer<>(context.getSkiffSerializer().get()));
            }
            if (context.isProtobufFormat()) {
                return Optional.of(castToType(new TableRowsProtobufSerializer<>()));
            }
            if (context.getYtreeSerializer().isEmpty()) {
                throw new IllegalArgumentException("No yson serializer for RF_FORMAT");
            }
            if (!context.getFormat().get().getType().equals(YSON)) {
                throw new IllegalArgumentException(
                        "Format " + context.getFormat().get().getType() + " isn't supported");
            }
            YTreeSerializer<T> serializer = context.getYtreeSerializer().get();
            return Optional.of(new TableRowsYsonSerializer<>(serializer));
        } else {
            throw new IllegalArgumentException("Unsupported rowset format");
        }
    }

    public ByteBuf serializeRowsToBuf(List<T> rows, TableSchema schema) {
        TRowsetDescriptor currentDescriptor = getCurrentRowsetDescriptor(schema);
        int[] idMapping = getIdMapping(rows, schema);

        ByteBuf buf = Unpooled.buffer();
        writeRowsWithoutCount(buf, currentDescriptor, rows, idMapping);

        updateRowsetDescriptor(currentDescriptor);

        return buf;
    }

    public byte[] serializeRowsWithDescriptor(ByteBuf serializedRows, int rowsCount) throws IOException {
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

    protected abstract void writeMeta(ByteBuf buf, ByteBuf serializedRows, int rowsCount);

    public byte[] serializeRows(List<T> rows, TableSchema schema) throws IOException {
        TRowsetDescriptor currentDescriptor = getCurrentRowsetDescriptor(schema);
        int[] idMapping = getIdMapping(rows, schema);

        ByteBuf buf = Unpooled.buffer();
        writeRowsDataWithDescriptor(buf, currentDescriptor, rows, idMapping);

        updateRowsetDescriptor(currentDescriptor);

        return bufToArray(buf);
    }

    protected TRowsetDescriptor getCurrentRowsetDescriptor(TableSchema schema) {
        TRowsetDescriptor.Builder builder = TRowsetDescriptor.newBuilder();

        for (ColumnSchema descriptor : schema.getColumns()) {
            if (!columnToId.containsKey(descriptor.getName())) {
                builder.addNameTableEntries(TRowsetDescriptor.TNameTableEntry.newBuilder()
                        .setName(descriptor.getName())
                        .setType(descriptor.getType().getValue())
                        .build());

                columnToId.put(descriptor.getName(), columnToId.size());
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
                ? new int[columnToId.size()]
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
                    int columnId = columnToId.get(columnName);
                    idMapping[value.getId()] = columnId;
                }
            }
        }

        return idMapping;
    }

    private void updateRowsetDescriptor(TRowsetDescriptor currentDescriptor) {
        if (currentDescriptor.getNameTableEntriesCount() <= 0) {
            return;
        }

        TRowsetDescriptor.Builder merged = TRowsetDescriptor.newBuilder();
        merged.setRowsetFormat(rowsetFormat);
        merged.mergeFrom(rowsetDescriptor);
        merged.addAllNameTableEntries(currentDescriptor.getNameTableEntriesList());
        rowsetDescriptor = merged.build();
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

    protected abstract void writeRowsWithoutCount(
            ByteBuf buf, TRowsetDescriptor descriptor, List<T> rows, int[] idMapping);

    private void writeRowsDataWithDescriptor(
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

        writeRows(buf, descriptorDelta, rows, idMapping);

        buf.setLongLE(mergedRowSizeIndex, buf.writerIndex() - mergedRowSizeIndex - 8);
    }

    protected abstract void writeRows(
            ByteBuf buf,
            TRowsetDescriptor descriptor,
            List<T> rows,
            int[] idMapping);
}
