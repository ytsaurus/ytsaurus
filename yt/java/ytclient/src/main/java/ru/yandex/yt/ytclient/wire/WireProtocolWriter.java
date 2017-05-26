package ru.yandex.yt.ytclient.wire;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.MessageLite;

import ru.yandex.yt.ytclient.tables.ColumnValueType;

public class WireProtocolWriter {
    private static final int INITIAL_BUFFER_CAPACITY = 1024;
    private static final int PREALLOCATE_BLOCK_SIZE = 4096;

    private final ChunkedWriter writer;
    private ByteBuffer current;
    private int currentStart;
    private long flushed;

    public WireProtocolWriter() {
        this(new ArrayList<>());
    }

    public WireProtocolWriter(List<byte[]> output) {
        this.writer = new ChunkedWriter(output);
        reserve(INITIAL_BUFFER_CAPACITY);
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
            flushed += current.position() - currentStart;
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
        current = ByteBuffer.wrap(writer.buffer(), writer.offset(), size).order(ByteOrder.LITTLE_ENDIAN);
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

    private int estimateNullBitmapByteSize(List<UnversionedValue> row) {
        return WireProtocol.alignUp(8 * Bitmap.computeChunkCount(row.size()));
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

    private int estimateSchemafulValueByteSize(UnversionedValue value) {
        if (value.getType().isStringLikeType()) {
            return WireProtocol.alignUp(4) + WireProtocol.alignUp(value.bytesValue().length);
        } else if (value.getType().isValueType()) {
            return WireProtocol.alignUp(8);
        } else {
            return 0;
        }
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
        }
    }

    private int estimateSchemafulValuesByteSize(List<UnversionedValue> values) {
        int size = estimateNullBitmapByteSize(values);
        for (UnversionedValue value : values) {
            size += estimateSchemafulValueByteSize(value);
        }
        return size;
    }

    private void writeSchemafulValues(List<UnversionedValue> values) {
        writeNullBitmap(values);
        for (UnversionedValue value : values) {
            writeSchemafulValue(value);
        }
    }

    private int estimateUnversionedValueByteSize(UnversionedValue value) {
        int size = WireProtocol.alignUp(8);
        if (value.getType().isStringLikeType()) {
            size += WireProtocol.alignUp(value.bytesValue().length);
        } else if (value.getType().isValueType()) {
            size += WireProtocol.alignUp(8);
        }
        return size;
    }

    private void writeUnversionedValueHeader(UnversionedValue value) {
        reserveAligned(8);
        current.putShort(WireProtocol.validateColumnId(value.getId()));
        current.put((byte) value.getType().getValue());
        current.put((byte) (value.isAggregate() ? 1 : 0));
        current.putInt(value.getLength());
        alignAfterWriting(8);
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
        }
    }

    private int estimateUnversionedValuesByteSize(List<UnversionedValue> values) {
        int size = 0;
        for (UnversionedValue value : values) {
            size += estimateUnversionedValueByteSize(value);
        }
        return size;
    }

    private void writeUnversionedValues(List<UnversionedValue> values) {
        for (UnversionedValue value : values) {
            writeUnversionedValue(value);
        }
    }

    private int estimateVersionedValueByteSize(VersionedValue value) {
        return estimateUnversionedValueByteSize(value) + WireProtocol.alignUp(8);
    }

    private void writeVersionedValue(VersionedValue value) {
        writeUnversionedValue(value);
        writeLong(value.getTimestamp());
    }

    private int estimateVersionedValuesByteSize(List<VersionedValue> values) {
        int size = 0;
        for (VersionedValue value : values) {
            size += estimateVersionedValueByteSize(value);
        }
        return size;
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

    private int estimateSchemafulRowByteSize(UnversionedRow row) {
        return WireProtocol.alignUp(8) + (row != null ? estimateSchemafulValuesByteSize(row.getValues()) : 0);
    }

    public void writeSchemafulRow(UnversionedRow row) {
        int size = estimateSchemafulRowByteSize(row);
        reserveAligned(size);
        if (row != null) {
            WireProtocol.validateRowValueCount(row.getValues().size());
            writeLong(row.getValues().size());
            writeSchemafulValues(row.getValues());
        } else {
            writeLong(-1);
        }
    }

    private int estimateUnversionedRowByteSize(UnversionedRow row) {
        return WireProtocol.alignUp(8) + (row != null ? estimateUnversionedValuesByteSize(row.getValues()) : 0);
    }

    public void writeUnversionedRow(UnversionedRow row) {
        int size = estimateUnversionedRowByteSize(row);
        reserveAligned(size);
        if (row != null) {
            WireProtocol.validateRowValueCount(row.getValues().size());
            writeLong(row.getValues().size());
            writeUnversionedValues(row.getValues());
        } else {
            writeLong(-1);
        }
    }

    private int estimateTimestampsByteSize(List<Long> timestamps) {
        return WireProtocol.alignUp(8 * timestamps.size());
    }

    private void writeTimestamps(List<Long> timestamps) {
        int byteCount = 8 * timestamps.size();
        reserveAligned(byteCount);
        for (long timestamp : timestamps) {
            current.putLong(timestamp);
        }
        alignAfterWriting(byteCount);
    }

    private int estimateVersionedRowByteSize(VersionedRow row) {
        int size = 0;
        if (row != null) {
            size += WireProtocol.alignUp(16);
            size += estimateTimestampsByteSize(row.getWriteTimestamps());
            size += estimateTimestampsByteSize(row.getDeleteTimestamps());
            size += estimateUnversionedValuesByteSize(row.getKeys());
            size += estimateVersionedValuesByteSize(row.getValues());
        } else {
            size += WireProtocol.alignUp(8);
        }
        return size;
    }

    public void writeVersionedRow(VersionedRow row) {
        int size = estimateVersionedRowByteSize(row);
        reserveAligned(size);
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

    private void writeRowCount(int rowCount) {
        WireProtocol.validateRowCount(rowCount);
        writeLong(rowCount);
    }

    public void writeSchemafulRowset(List<UnversionedRow> rows) {
        writeRowCount(rows.size());
        for (UnversionedRow row : rows) {
            writeSchemafulRow(row);
        }
    }

    public void writeUnversionedRowset(List<UnversionedRow> rows) {
        writeRowCount(rows.size());
        for (UnversionedRow row : rows) {
            writeUnversionedRow(row);
        }
    }

    public void writeVersionedRowset(List<VersionedRow> rows) {
        writeRowCount(rows.size());
        for (VersionedRow row : rows) {
            writeVersionedRow(row);
        }
    }
}
