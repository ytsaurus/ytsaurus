package tech.ytsaurus.client.rows;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.MessageLite;
import tech.ytsaurus.core.tables.ColumnValueType;

public class WireProtocolWriter {
    private static final int INITIAL_BUFFER_CAPACITY = 1024;
    private static final int PREALLOCATE_BLOCK_SIZE = 4096;

    private final WireProtocolWriteable writeable;
    private final ChunkedWriter writer;
    private ByteBuffer current;
    private int currentStart;

    public WireProtocolWriter() {
        this(new ArrayList<>());
    }

    public WireProtocolWriter(List<byte[]> output) {
        this(output, ChunkedWriter.MAX_CHUNK_SIZE);
    }

    WireProtocolWriter(List<byte[]> output, int limitChunkSize) {
        this.writer = new ChunkedWriter(output, 0, limitChunkSize);
        reserve(INITIAL_BUFFER_CAPACITY);

        this.writeable = new WireProtocolWriteable() {

            @Override
            public void onEntity() {
                throw new IllegalStateException("Value must be provided");
            }

            @Override
            public void onInteger(long value) {
                writeLong(value);
            }

            @Override
            public void onBoolean(boolean value) {
                writeLong(value ? 1 : 0);
            }

            @Override
            public void onDouble(double value) {
                writeLong(Double.doubleToRawLongBits(value));
            }

            @Override
            public void onBytes(byte[] bytes) {
                writeBytes(bytes);
            }

            @Override
            public void writeValueCount(int valueCount) {
                reserveAligned(8);
                writer.mark(current.position());
                writeLong(WireProtocol.validateRowValueCount(valueCount));
            }

            @Override
            public void overwriteValueCount(int valueCount) {
                writer.getMarker().writeToMark(current, ByteOrder.LITTLE_ENDIAN,
                        WireProtocol.validateRowValueCount(valueCount));
            }

            @Override
            public void writeValueHeader(int columnId, ColumnValueType type, boolean aggregate, int length) {
                writeUnversionedValueHeader(columnId, type, aggregate, length);
            }

        };
    }

    /**
     * Заканчивает запись данных и возвращает сформированный список чанков
     */
    public List<byte[]> finish() {
        flushCurrent();
        return writer.flush();
    }

    private void flushCurrent() {
        if (current != null) {
            writer.advance(current.position() - currentStart);
            current = null;
        }
    }

    private void reserve(int size) {
        if (current != null && current.remaining() >= size) {
            // В буфере ещё достаточно места
            return;
        }
        flushCurrent();
        writer.reserve(Math.max(size, PREALLOCATE_BLOCK_SIZE));
        current = ByteBuffer.wrap(writer.buffer(), writer.offset(), writer.remaining()).order(ByteOrder.LITTLE_ENDIAN);
        currentStart = current.position();
    }

    private void reserveAligned(int size) {
        reserve(WireProtocol.alignUp(size));
    }

    private void alignAfterWriting(int size) {
        current.position(current.position() + WireProtocol.alignTail(size));
    }

    private void writeLong(long value) {
        reserveAligned(8);
        current.putLong(value);
        alignAfterWriting(8);
    }

    private void writeBytes(byte[] value) {
        reserveAligned(value.length);
        current.put(value);
        alignAfterWriting(value.length);
    }

    private void writeNullBitmap(List<UnversionedValue> row) {
        Bitmap bitmap = new Bitmap(row.size());
        for (int i = 0; i < row.size(); i++) {
            if (row.get(i).getType() == ColumnValueType.NULL) {
                bitmap.setBit(i);
            }
        }
        int byteCount = 8 * bitmap.getChunkCount();
        reserveAligned(byteCount);
        for (int i = 0; i < bitmap.getChunkCount(); i++) {
            current.putLong(bitmap.getChunk(i));
        }
        alignAfterWriting(byteCount);
    }

    private void writeSchemafulValue(UnversionedValue value) {
        switch (value.getType()) {
            case INT64:
            case UINT64:
            case DOUBLE:
            case BOOLEAN:
                writeLong(value.toRawBits());
                break;
            case STRING:
            case ANY:
                byte[] data = value.bytesValue();
                writeLong(data.length);
                writeBytes(data);
                break;
            default:
                break;
        }
    }

    private void writeSchemafulValues(List<UnversionedValue> values) {
        writeNullBitmap(values);
        for (UnversionedValue value : values) {
            writeSchemafulValue(value);
        }
    }

    public void writeUnversionedValueHeader(int columnId, ColumnValueType type, boolean aggregate, int length) {
        reserveAligned(8);
        current.putShort(WireProtocol.validateColumnId(columnId));
        current.put((byte) type.getValue());
        current.put((byte) (aggregate ? WireProtocol.AGGREGATE_FLAG : 0));
        current.putInt(length);
        alignAfterWriting(8);
    }

    private void writeUnversionedValueHeader(UnversionedValue value) {
        this.writeUnversionedValueHeader(value.getId(), value.getType(), value.isAggregate(), value.getLength());
    }

    private void writeUnversionedValue(UnversionedValue value) {
        writeUnversionedValueHeader(value);
        switch (value.getType()) {
            case INT64:
            case UINT64:
            case DOUBLE:
            case BOOLEAN:
                writeLong(value.toRawBits());
                break;
            case STRING:
            case ANY:
                writeBytes(value.bytesValue());
                break;
            default:
                break;
        }
    }

    private void writeVersionedValue(VersionedValue value) {
        writeUnversionedValue(value);
        writeLong(value.getTimestamp());
    }

    private void writeVersionedValues(List<VersionedValue> values) {
        for (VersionedValue value : values) {
            writeVersionedValue(value);
        }
    }

    public void writeMessage(MessageLite message) {
        int size = message.getSerializedSize();
        writeLong(size);
        reserveAligned(size);
        try {
            message.writeTo(CodedOutputStream
                    .newInstance(current.array(), current.arrayOffset() + current.position(), size));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        current.position(current.position() + size);
        alignAfterWriting(size);
    }

    public void writeSchemafulRow(UnversionedRow row) {
        if (row != null) {
            writeLong(WireProtocol.validateRowValueCount(row.getValues().size()));
            writeSchemafulValues(row.getValues());
        } else {
            writeLong(-1);
        }
    }

    public <T> void writeUnversionedRow(T row, WireRowSerializer<T> serializer) {
        writeUnversionedRow(row, serializer, (int[]) null);
    }

    public <T> void writeUnversionedRow(T row, WireRowSerializer<T> serializer, int[] idMapping) {
        writeUnversionedRow(row, serializer, false, false, idMapping);
    }

    public <T> void writeUnversionedRow(T row, WireRowSerializer<T> serializer, boolean keyFieldsOnly) {
        writeUnversionedRow(row, serializer, keyFieldsOnly, false, (int[]) null);
    }

    public <T> void writeUnversionedRow(
            T row,
            WireRowSerializer<T> serializer,
            boolean keyFieldsOnly,
            boolean aggregate,
            int[] idMapping
    ) {
        if (row != null) {
            serializer.serializeRow(row, this.writeable, keyFieldsOnly, aggregate, idMapping);
        } else {
            writeLong(-1);
        }
    }

    private void writeTimestamps(List<Long> timestamps) {
        int byteCount = 8 * timestamps.size();
        reserveAligned(byteCount);
        for (long timestamp : timestamps) {
            current.putLong(timestamp);
        }
        alignAfterWriting(byteCount);
    }

    public void writeVersionedRow(VersionedRow row) {
        if (row != null) {
            WireProtocol.validateRowKeyCount(row.getKeys().size());
            WireProtocol.validateRowValueCount(row.getValues().size());
            WireProtocol.validateRowValueCount(row.getWriteTimestamps().size());
            WireProtocol.validateRowValueCount(row.getDeleteTimestamps().size());

            // TVersionedRowHeader
            reserveAligned(16);
            current.putInt(row.getValues().size());
            current.putInt(row.getKeys().size());
            current.putInt(row.getWriteTimestamps().size());
            current.putInt(row.getDeleteTimestamps().size());
            alignAfterWriting(16);

            writeTimestamps(row.getWriteTimestamps());
            writeTimestamps(row.getDeleteTimestamps());
            writeSchemafulValues(row.getKeys());
            writeVersionedValues(row.getValues());
        } else {
            writeLong(-1);
        }
    }

    public void writeRowCount(int rowCount) {
        writeLong(WireProtocol.validateRowCount(rowCount));
    }

    public void writeSchemafulRowset(List<UnversionedRow> rows) {
        writeRowCount(rows.size());
        for (UnversionedRow row : rows) {
            writeSchemafulRow(row);
        }
    }

    public <T> void writeUnversionedRowset(List<T> rows, WireRowSerializer<T> serializer) {
        writeUnversionedRowset(rows, serializer, (int[]) null);
    }

    public <T> void writeUnversionedRowset(List<T> rows, WireRowSerializer<T> serializer, int[] idMapping) {
        writeUnversionedRowset(rows, serializer, (i) -> false, (i) -> false, idMapping);
    }

    public <T> void writeUnversionedRowsetWithoutCount(List<T> rows, WireRowSerializer<T> serializer, int[] idMapping) {
        for (T row : rows) {
            writeUnversionedRow(row, serializer, idMapping);
        }
    }

    public <T> void writeUnversionedRowset(List<T> rows, WireRowSerializer<T> serializer,
                                           Function<Integer, Boolean> keyFieldsOnlyFunction,
                                           Function<Integer, Boolean> isAggregateFunction) {
        writeUnversionedRowset(rows, serializer, keyFieldsOnlyFunction, isAggregateFunction, null);
    }

    public <T> void writeUnversionedRowset(List<T> rows, WireRowSerializer<T> serializer,
                                           Function<Integer, Boolean> func) {
        writeUnversionedRowset(rows, serializer, func, (i) -> false, null);
    }

    public <T> void writeUnversionedRowset(
            List<T> rows,
            WireRowSerializer<T> serializer,
            Function<Integer, Boolean> isKeyFieldsOnlyFunction,
            Function<Integer, Boolean> isAggregateFunction,
            int[] idMapping
    ) {
        final int rowCount = rows.size();
        writeRowCount(rowCount);
        for (int i = 0; i < rowCount; i++) {
            writeUnversionedRow(rows.get(i), serializer, isKeyFieldsOnlyFunction.apply(i),
                    isAggregateFunction.apply(i), idMapping);
        }
    }

    public void writeVersionedRowset(List<VersionedRow> rows) {
        writeRowCount(rows.size());
        for (VersionedRow row : rows) {
            writeVersionedRow(row);
        }
    }
}
