package tech.ytsaurus.client;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.Message;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
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
public interface TableRowsSerializer<T> {
    /**
     * Returns rowset descriptor.
     */
    TRowsetDescriptor getRowsetDescriptor();

    /**
     * Serialize and write rows to the internal buffer.
     *
     * @param rows batch of rows.
     */
    void write(List<T> rows);

    /**
     * Returns the current size of the internal buffer with serialized rows plus the size of the metadata.
     * <p>
     * The value returned by this method is equal to the size of the data that would be returned by
     * the next {@link #flush()} call (without {@link #write(List)} calls in between).
     */
    int size();

    /**
     * Returns an InputStream containing serialized rows with metadata, and clears the internal buffer.
     */
    InputStream flush();
}

@NonNullApi
@NonNullFields
abstract class TableRowsSerializerBase<T> implements TableRowsSerializer<T> {
    protected TRowsetDescriptor rowsetDescriptor;
    protected ByteBuf serializedRows;

    TableRowsSerializerBase(ERowsetFormat rowsetFormat) {
        this.rowsetDescriptor = TRowsetDescriptor.newBuilder().setRowsetFormat(rowsetFormat).build();
        this.serializedRows = Unpooled.buffer();
    }

    public TRowsetDescriptor getRowsetDescriptor() {
        return rowsetDescriptor;
    }

    @Override
    public InputStream flush() {
        if (serializedRows.readableBytes() == 0) {
            return InputStream.nullInputStream();
        }
        ByteBuf metaBuf = Unpooled.buffer();
        writeMeta(metaBuf);
        ByteBuf currentSerializedRows = serializedRows;
        this.serializedRows = Unpooled.buffer();
        return new SequenceInputStream(new ByteBufInputStream(metaBuf), new ByteBufInputStream(currentSerializedRows));
    }

    @Override
    public int size() {
        if (serializedRows.readableBytes() == 0) {
            return 0;
        }
        return serializedRows.readableBytes() + getMetaSize();
    }

    protected abstract void writeMeta(ByteBuf buf);

    protected abstract int getMetaSize();
}

class TableRowsSerializerUtil {
    private static final String YSON = "yson";

    private TableRowsSerializerUtil() {
    }

    static <T> Optional<TableRowsSerializer<T>> createTableRowsSerializer(
            SerializationContext<T> context,
            SerializationResolver serializationResolver
    ) {
        if (context.getTableRowsSerializer().isPresent()) {
            return context.getTableRowsSerializer();
        }
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

    static <T> byte[] serializeRowsWithDescriptor(
            TableRowsSerializer<T> serializer,
            TRowsetDescriptor descriptor
    ) throws IOException {
        ByteBuf buf = Unpooled.buffer();

        // parts
        buf.writeIntLE(2);

        int descriptorSizeIndex = buf.writerIndex();
        buf.writeLongLE(0); // reserve space

        writeDescriptor(buf, descriptor);

        buf.setLongLE(descriptorSizeIndex, buf.writerIndex() - descriptorSizeIndex - 8);

        int bufSize = buf.readableBytes();

        // Convert to array.
        int serializedRowsSize = serializer.size();
        byte[] result = new byte[bufSize + serializedRowsSize];
        buf.readBytes(result, 0, bufSize);
        if (buf.readableBytes() != 0) {
            throw new IllegalStateException();
        }

        // Write serialized rows data.
        try (InputStream serializedRows = serializer.flush()) {
            int readBytes;
            int offset = bufSize;
            while (offset != result.length &&
                    (readBytes = serializedRows.read(result, offset, result.length - offset)) != -1) {
                offset += readBytes;
            }
        }

        return result;
    }

    private static void writeDescriptor(ByteBuf buf, TRowsetDescriptor descriptor) throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        CodedOutputStream os = CodedOutputStream.newInstance(byteArrayOutputStream);
        descriptor.writeTo(os);
        os.flush();

        buf.writeBytes(byteArrayOutputStream.toByteArray());
    }
}

@NonNullApi
@NonNullFields
class TableRowsWireSerializer<T> extends TableRowsSerializerBase<T> {
    private final WireRowSerializer<T> wireRowSerializer;
    private final Map<String, Integer> columnToId = new HashMap<>();
    private final ERowsetFormat rowsetFormat = ERowsetFormat.RF_YT_WIRE;
    private int rowsCount;

    TableRowsWireSerializer(WireRowSerializer<T> wireRowSerializer) {
        super(ERowsetFormat.RF_YT_WIRE);
        this.wireRowSerializer = Objects.requireNonNull(wireRowSerializer);
    }

    public TableSchema getSchema() {
        return wireRowSerializer.getSchema();
    }

    @Override
    public void write(List<T> rows) {
        throw new RuntimeException(new NoSuchMethodException());
    }

    @Override
    public InputStream flush() {
        InputStream in = super.flush();
        this.rowsCount = 0;
        return in;
    }

    void write(List<T> rows, TableSchema schema) {
        write(rows, schema, getCurrentRowsetDescriptor(schema));
    }

    void write(List<T> rows, TableSchema schema, TRowsetDescriptor currentDescriptor) {
        int[] idMapping = getIdMapping(rows, schema);

        WireProtocolWriter writer = new WireProtocolWriter();
        wireRowSerializer.updateSchema(currentDescriptor);
        writer.writeUnversionedRowsetWithoutCount(rows, wireRowSerializer, idMapping);

        for (byte[] bytes : writer.finish()) {
            serializedRows.writeBytes(bytes);
        }
        updateRowsetDescriptor(currentDescriptor);
        rowsCount += rows.size();
    }

    @Override
    protected void writeMeta(ByteBuf buf) {
        int mergedRowSizeIndex = buf.writerIndex();
        buf.writeLongLE(0); // reserve space

        // Write rows count.
        WireProtocolWriter writer = new WireProtocolWriter();
        writer.writeRowCount(rowsCount);
        for (byte[] bytes : writer.finish()) {
            buf.writeBytes(bytes);
        }

        // Save serialized rows data size.
        buf.setLongLE(mergedRowSizeIndex,
                serializedRows.readableBytes() + (buf.writerIndex() - mergedRowSizeIndex) - 8);
    }

    @Override
    protected int getMetaSize() {
        int rowsCountSize = 0;
        WireProtocolWriter writer = new WireProtocolWriter();
        writer.writeRowCount(rowsCount);
        for (byte[] bytes : writer.finish()) {
            rowsCountSize += bytes.length;
        }
        return Long.BYTES + rowsCountSize;
    }

    TRowsetDescriptor getCurrentRowsetDescriptor(TableSchema schema) {
        TRowsetDescriptor.Builder builder = TRowsetDescriptor.newBuilder();

        for (ColumnSchema descriptor : schema.getColumns()) {
            if (!columnToId.containsKey(descriptor.getName())) {
                builder.addNameTableEntries(TRowsetDescriptor.TNameTableEntry.newBuilder()
                        .setName(descriptor.getName())
                        .setType(descriptor.getWireType().getValue())
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
}

@NonNullApi
@NonNullFields
class TableRowsYsonSerializer<T> extends TableRowsSerializerBase<T> {
    private final YTreeSerializer<T> ysonSerializer;

    TableRowsYsonSerializer(YTreeSerializer<T> ysonSerializer) {
        super(ERowsetFormat.RF_FORMAT);
        this.ysonSerializer = ysonSerializer;
    }

    @Override
    public void write(List<T> rows) {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        YTreeBinarySerializer.serializeAllObjects(rows, ysonSerializer, output);
        serializedRows.writeBytes(output.toByteArray());
    }

    @Override
    protected void writeMeta(ByteBuf buf) {
        buf.writeLongLE(serializedRows.readableBytes());
    }

    @Override
    protected int getMetaSize() {
        return Long.BYTES;
    }
}

@NonNullApi
@NonNullFields
class TableRowsSkiffSerializer<T> extends TableRowsSerializerBase<T> {
    private final EntitySkiffSerializer<T> serializer;

    TableRowsSkiffSerializer(EntitySkiffSerializer<T> serializer) {
        super(ERowsetFormat.RF_FORMAT);
        this.serializer = serializer;
    }

    @Override
    public void write(List<T> rows) {
        rows.forEach(row -> {
            serializedRows.writeByte(0);
            serializedRows.writeByte(0);
            serializedRows.writeBytes(serializer.serialize(row));
        });
    }

    @Override
    protected void writeMeta(ByteBuf buf) {
        buf.writeLongLE(serializedRows.readableBytes());
    }

    @Override
    protected int getMetaSize() {
        return Long.BYTES;
    }
}

@NonNullApi
@NonNullFields
class TableRowsProtobufSerializer<T extends Message> extends TableRowsSerializerBase<T> {
    TableRowsProtobufSerializer() {
        super(ERowsetFormat.RF_FORMAT);
    }

    @Override
    public void write(List<T> rows) {
        rows.forEach(row -> {
            byte[] messageBytes = row.toByteArray();
            serializedRows.writeIntLE(messageBytes.length);
            serializedRows.writeBytes(messageBytes);
        });
    }

    @Override
    protected void writeMeta(ByteBuf buf) {
        buf.writeLongLE(serializedRows.readableBytes());
    }

    @Override
    protected int getMetaSize() {
        return Long.BYTES;
    }
}
