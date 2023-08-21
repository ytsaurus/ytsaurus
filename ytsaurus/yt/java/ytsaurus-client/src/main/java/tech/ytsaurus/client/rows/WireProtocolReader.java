package tech.ytsaurus.client.rows;

import java.nio.BufferUnderflowException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.IntConsumer;
import java.util.function.LongSupplier;

import tech.ytsaurus.core.tables.ColumnSchema;
import tech.ytsaurus.core.tables.ColumnValueType;
import tech.ytsaurus.core.tables.TableSchema;

public class WireProtocolReader {
    private static final byte[] EMPTY_BUFFER = new byte[0];

    private final List<byte[]> chunks;
    private int nextChunk;
    private byte[] current;
    private int offset;

    public WireProtocolReader(List<byte[]> chunks) {
        this.chunks = chunks;
    }

    /**
     * Возвращает true, если из потока можно прочитать хотя бы 1 байт
     */
    public boolean readable() {
        // Пропускаем все null, пустые или закончившиеся чанки
        while (current == null || offset >= current.length) {
            if (nextChunk >= chunks.size()) {
                return false;
            }
            current = chunks.get(nextChunk++);
            offset = 0;
        }
        return true;
    }

    /**
     * Находит следующий читаемый байт в потоке и возвращает кол-во байт, которое можно прочитать из current
     */
    private int ensureReadable() {
        if (!readable()) {
            throw new BufferUnderflowException();
        }
        return current.length - offset;
    }

    /**
     * Пропускает count сырых байт в потоке чанков
     */
    private void skipRawBytes(int count) {
        while (count > 0) {
            int readable = ensureReadable();
            if (readable >= count) {
                offset += count;
                return;
            }
            offset += readable;
            count -= readable;
        }
    }

    /**
     * Читает сырой byte из потока чанков
     */
    private byte readRawByte() {
        ensureReadable();
        return current[offset++];
    }

    /**
     * Читает сырой short из потока чанков
     */
    private short readRawShort() {
        int readable = ensureReadable();
        if (readable < 2) {
            // short распилило между чанками, читаем побайтово
            return (short) (Byte.toUnsignedInt(readRawByte()) |
                    Byte.toUnsignedInt(readRawByte()) << 8);
        } else {
            int value = (Byte.toUnsignedInt(current[offset]) |
                    Byte.toUnsignedInt(current[offset + 1]) << 8);
            offset += 2;
            return (short) value;
        }
    }

    /**
     * Читает сырой int из потока чанков
     */
    private int readRawInt() {
        int readable = ensureReadable();
        if (readable < 4) {
            // int распилило между чанками, читаем побайтово
            return (Byte.toUnsignedInt(readRawByte()) |
                    Byte.toUnsignedInt(readRawByte()) << 8 |
                    Byte.toUnsignedInt(readRawByte()) << 16 |
                    Byte.toUnsignedInt(readRawByte()) << 24);
        } else {
            int value = (Byte.toUnsignedInt(current[offset]) |
                    Byte.toUnsignedInt(current[offset + 1]) << 8 |
                    Byte.toUnsignedInt(current[offset + 2]) << 16 |
                    Byte.toUnsignedInt(current[offset + 3]) << 24);
            offset += 4;
            return value;
        }
    }

    private long readRawLong() {
        int readable = ensureReadable();
        if (readable < 8) {
            // long распилило межу чанками, читаем побайтово
            return (Byte.toUnsignedLong(readRawByte()) |
                    Byte.toUnsignedLong(readRawByte()) << 8 |
                    Byte.toUnsignedLong(readRawByte()) << 16 |
                    Byte.toUnsignedLong(readRawByte()) << 24 |
                    Byte.toUnsignedLong(readRawByte()) << 32 |
                    Byte.toUnsignedLong(readRawByte()) << 40 |
                    Byte.toUnsignedLong(readRawByte()) << 48 |
                    Byte.toUnsignedLong(readRawByte()) << 56);
        } else {
            long value = (Byte.toUnsignedLong(current[offset]) |
                    Byte.toUnsignedLong(current[offset + 1]) << 8 |
                    Byte.toUnsignedLong(current[offset + 2]) << 16 |
                    Byte.toUnsignedLong(current[offset + 3]) << 24 |
                    Byte.toUnsignedLong(current[offset + 4]) << 32 |
                    Byte.toUnsignedLong(current[offset + 5]) << 40 |
                    Byte.toUnsignedLong(current[offset + 6]) << 48 |
                    Byte.toUnsignedLong(current[offset + 7]) << 56);
            offset += 8;
            return value;
        }
    }

    private void alignAfterReading(int size) {
        skipRawBytes(WireProtocol.alignTail(size));
    }

    private long readLong() {
        long result = readRawLong();
        alignAfterReading(8);
        return result;
    }

    private byte[] readBytes(int length) {
        byte[] result = new byte[length];
        int index = 0;
        while (index < length) {
            int available = Math.min(ensureReadable(), length - index);
            System.arraycopy(current, offset, result, index, available);
            offset += available;
            index += available;
        }
        alignAfterReading(length);
        return result;
    }

    private byte[] readStringData(ColumnValueType type, long length) {
        int limit = 0;
        switch (type) {
            case STRING:
                limit = WireProtocol.MAX_STRING_VALUE_LENGTH;
                break;
            case ANY:
                limit = WireProtocol.MAX_ANY_VALUE_LENGTH;
                break;
            case COMPOSITE:
                limit = WireProtocol.MAX_COMPOSITE_VALUE_LENGTH;
                break;
            default:
                break;
        }
        if (length < 0 || length > limit) {
            throw new IllegalStateException("Unsupported " + type + " data length " + length);
        }
        return readBytes((int) length);
    }

    private Bitmap readNullBitmap(int count) {
        Bitmap result = new Bitmap(count);
        int byteCount = 0;
        for (int i = 0; i < result.getChunkCount(); i++) {
            result.setChunk(i, readRawLong());
            byteCount += 8;
        }
        alignAfterReading(byteCount);
        return result;
    }

    private void readUnversionedValues(WireValueDeserializer<?> consumer, int valueCount) {
        for (int i = 0; i < valueCount; i++) {
            readValue(() -> -1, consumer);
        }
    }

    private void readSchemafulValues(
            WireValueDeserializer<?> consumer,
            List<WireColumnSchema> schemaData,
            int valueCount
    ) {
        final Bitmap nullBitmap = readNullBitmap(valueCount);
        for (int index = 0; index < valueCount; index++) {
            final WireColumnSchema column = schemaData.get(index);
            consumer.setId(column.getId());
            consumer.setAggregate(column.isAggregate());

            final boolean isNull = nullBitmap.getBit(index);
            final ColumnValueType type = isNull ? ColumnValueType.NULL : column.getType();

            readValueImpl(consumer, type, Integer.MAX_VALUE);
            consumer.build();
        }
    }

    private void readVersionedValues(WireValueDeserializer<?> consumer, int valueCount) {
        for (int i = 0; i < valueCount; i++) {
            readValue(this::readLong, consumer);
        }
    }

    private <T> void readValueImpl(WireValueDeserializer<T> consumer, ColumnValueType type, int length) {
        consumer.setType(type);
        if (type.isStringLikeType()) {
            final byte[] bytes = readStringData(type, length == Integer.MAX_VALUE ? readLong() : length);
            consumer.onBytes(bytes);
        } else if (type.isValueType()) {
            final long rawBits = readLong();
            switch (type) {
                case INT64:
                case UINT64:
                    consumer.onInteger(rawBits);
                    break;
                case DOUBLE:
                    consumer.onDouble(Double.longBitsToDouble(rawBits));
                    break;
                case BOOLEAN:
                    // bool is byte-sized in C++, have to be careful about random garbage
                    consumer.onBoolean((rawBits & 0xff) != 0);
                    break;
                default:
                    throw new IllegalArgumentException(type + " cannot be represented as raw bits");
            }
        } else {
            consumer.onEntity(); // no value
        }
    }

    private <T> void readValue(LongSupplier timestampSupplier, WireValueDeserializer<T> consumer) {
        final int id = readRawShort() & 0xffff;
        consumer.setId(id);

        final ColumnValueType type = ColumnValueType.fromValue(readRawByte() & 0xff);

        final byte flags = readRawByte();
        final boolean aggregate = (flags & WireProtocol.AGGREGATE_FLAG) != 0;
        consumer.setAggregate(aggregate);

        final int length = readRawInt();
        alignAfterReading(8);

        this.readValueImpl(consumer, type, length);

        consumer.setTimestamp(timestampSupplier.getAsLong());
        consumer.build();
    }

    private List<Long> readTimestamps(int count) {
        List<Long> result = new ArrayList<>(count);
        int byteCount = 0;
        for (int i = 0; i < count; i++) {
            result.add(readRawLong());
            byteCount += 8;
        }
        alignAfterReading(byteCount);
        return result;
    }

    public <T> T readSchemafulRow(WireSchemafulRowDeserializer<T> deserializer) {
        long valueCount = readLong();
        if (valueCount == -1) {
            return null;
        }
        final int valueCountInt = WireProtocol.validateRowValueCount(valueCount);
        readSchemafulValues(deserializer.onNewRow(valueCountInt), deserializer.getColumnSchema(), valueCountInt);
        return deserializer.onCompleteRow();
    }

    public <T> T readUnversionedRow(WireRowDeserializer<T> deserializer) {
        final long valueCount0 = readLong();
        if (valueCount0 == -1) {
            return deserializer.onNullRow();
        }
        final int valueCount = WireProtocol.validateRowValueCount(valueCount0);
        final WireValueDeserializer<?> consumer = deserializer.onNewRow(valueCount);
        readUnversionedValues(consumer, valueCount);
        return deserializer.onCompleteRow();
    }

    public <T> T readVersionedRow(WireVersionedRowDeserializer<T> deserializer) {
        // TVersionedRowHeader
        final int valueCount = readRawInt();
        final int keyCount = readRawInt();
        if (valueCount == -1 && keyCount == -1) {
            alignAfterReading(8);
            return null;
        }
        final int writeTimestampCount = readRawInt();
        final int deleteTimestampCount = readRawInt();
        alignAfterReading(16);

        WireProtocol.validateRowKeyCount(keyCount);
        WireProtocol.validateRowValueCount(valueCount);
        WireProtocol.validateRowValueCount(writeTimestampCount);
        WireProtocol.validateRowValueCount(deleteTimestampCount);

        deserializer.onWriteTimestamps(readTimestamps(writeTimestampCount));
        deserializer.onDeleteTimestamps(readTimestamps(deleteTimestampCount));

        final WireValueDeserializer<?> keys = deserializer.keys(keyCount);
        readSchemafulValues(keys, deserializer.getKeyColumnSchema(), keyCount);

        final WireValueDeserializer<?> values = deserializer.values(valueCount);
        readVersionedValues(values, valueCount);

        return deserializer.onCompleteRow();
    }

    public int readRowCount() {
        return WireProtocol.validateRowCount(readLong());
    }

    public void skipRowCountHeader() {
        readRowCount();
    }

    public <T extends WireRowsetDeserializer<V>, V> T readUnversionedRowset(T deserializer) {
        return this.readImpl(deserializer, deserializer::setRowCount, this::readUnversionedRow);
    }

    public <T extends WireSchemafulRowsetDeserializer<V>, V> T readSchemafulRowset(T deserializer) {
        return this.readImpl(deserializer, deserializer::setRowCount, this::readSchemafulRow);
    }

    public <T extends WireVersionedRowsetDeserializer<V>, V> T readVersionedRowset(T deserializer) {
        return this.readImpl(deserializer, deserializer::setRowCount, this::readVersionedRow);
    }

    private <T> T readImpl(T deserializer, IntConsumer rowCountFunction, Consumer<T> readFunction) {
        final int rowCount = readRowCount();
        rowCountFunction.accept(rowCount);
        for (int i = 0; i < rowCount; i++) {
            readFunction.accept(deserializer);
        }
        return deserializer;
    }

    public static List<WireColumnSchema> makeSchemaData(TableSchema schema) {
        List<ColumnSchema> columns = schema.getColumns();
        List<WireColumnSchema> schemaData = new ArrayList<>(columns.size());
        for (int id = 0; id < columns.size(); ++id) {
            ColumnSchema column = columns.get(id);
            // См. TWireProtocolReader::GetSchemaData
            // При конструировании заголовка поле aggregate остаётся false
            schemaData.add(new WireColumnSchema(id, column.getType()));
        }
        return schemaData;
    }
}
