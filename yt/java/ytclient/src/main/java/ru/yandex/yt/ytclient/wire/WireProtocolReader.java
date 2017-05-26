package ru.yandex.yt.ytclient.wire;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.BufferUnderflowException;
import java.util.ArrayList;
import java.util.List;

import com.google.protobuf.CodedInputStream;

import ru.yandex.yt.ytclient.rpc.RpcMessageParser;
import ru.yandex.yt.ytclient.tables.ColumnSchema;
import ru.yandex.yt.ytclient.tables.ColumnValueType;
import ru.yandex.yt.ytclient.tables.TableSchema;

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

    private UnversionedValue readSchemafulValue(int id, ColumnValueType type, boolean aggregate, boolean isNull) {
        Object rawValue = null;
        if (isNull) {
            type = ColumnValueType.NULL;
        } else if (type.isStringLikeType()) {
            rawValue = readStringData(type, readLong());
        } else if (type.isValueType()) {
            long rawBits = readLong();
            rawValue = UnversionedValue.convertRawBitsTo(rawBits, type);
        }
        return new UnversionedValue(id, type, aggregate, rawValue);
    }

    private List<UnversionedValue> readSchemafulValues(List<WireColumnSchema> schemaData, int valueCount) {
        Bitmap nullBitmap = readNullBitmap(valueCount);
        List<UnversionedValue> values = new ArrayList<>(valueCount);
        for (int index = 0; index < valueCount; index++) {
            WireColumnSchema column = schemaData.get(index);
            values.add(readSchemafulValue(column.getId(), column.getType(), column.isAggregate(),
                    nullBitmap.getBit(index)));
        }
        return values;
    }

    private Object readUnversionedData(ColumnValueType type, int length) {
        if (type.isStringLikeType()) {
            return readStringData(type, length);
        } else if (type.isValueType()) {
            long rawBits = readLong();
            return UnversionedValue.convertRawBitsTo(rawBits, type);
        } else {
            return null;
        }
    }

    private UnversionedValue readUnversionedValue() {
        int id = readRawShort() & 0xffff;
        ColumnValueType type = ColumnValueType.fromValue(readRawByte() & 0xff);
        boolean aggregate = readRawByte() != 0;
        int length = readRawInt();
        alignAfterReading(8);
        Object value = readUnversionedData(type, length);
        return new UnversionedValue(id, type, aggregate, value);
    }

    private List<UnversionedValue> readUnversionedValues(int valueCount) {
        List<UnversionedValue> values = new ArrayList<>(valueCount);
        for (int i = 0; i < valueCount; i++) {
            values.add(readUnversionedValue());
        }
        return values;
    }

    private VersionedValue readVersionedValue() {
        int id = readRawShort() & 0xffff;
        ColumnValueType type = ColumnValueType.fromValue(readRawByte() & 0xff);
        boolean aggregate = readRawByte() != 0;
        int length = readRawInt();
        alignAfterReading(8);
        Object value = readUnversionedData(type, length);
        long timestamp = readLong();
        return new VersionedValue(id, type, aggregate, value, timestamp);
    }

    private List<VersionedValue> readVersionedValues(int valueCount) {
        List<VersionedValue> values = new ArrayList<>(valueCount);
        for (int i = 0; i < valueCount; i++) {
            values.add(readVersionedValue());
        }
        return values;
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

    public <T> T readMessage(RpcMessageParser<T> parser) {
        int size = (int) readLong();
        try {
            if (size == 0) {
                // Пустое сообщение, вызываем декодер на пустом буфере
                return parser.parse(CodedInputStream.newInstance(EMPTY_BUFFER));
            }
            int available = ensureReadable();
            if (available >= size) {
                // Декодируем сообщение из буфера одним куском
                T result = parser.parse(CodedInputStream.newInstance(current, offset, size));
                offset += size;
                alignAfterReading(size);
                return result;
            }
            // Собираем данные в массив байт и декодируем из него
            return parser.parse(CodedInputStream.newInstance(readBytes(size)));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public UnversionedRow readSchemafulRow(List<WireColumnSchema> schemaData) {
        long valueCount = readLong();
        if (valueCount == -1) {
            return null;
        }
        List<UnversionedValue> values = readSchemafulValues(schemaData, WireProtocol.validateRowValueCount(valueCount));
        return new UnversionedRow(values);
    }

    public UnversionedRow readUnversionedRow() {
        long valueCount = readLong();
        if (valueCount == -1) {
            return null;
        }
        List<UnversionedValue> values = readUnversionedValues(WireProtocol.validateRowValueCount(valueCount));
        return new UnversionedRow(values);
    }

    public VersionedRow readVersionedRow(List<WireColumnSchema> schemaData) {
        // TVersionedRowHeader
        int valueCount = readRawInt();
        int keyCount = readRawInt();
        if (valueCount == -1 && keyCount == -1) {
            alignAfterReading(8);
            return null;
        }
        int writeTimestampCount = readRawInt();
        int deleteTimestampCount = readRawInt();
        alignAfterReading(16);

        WireProtocol.validateRowKeyCount(keyCount);
        WireProtocol.validateRowValueCount(valueCount);
        WireProtocol.validateRowValueCount(writeTimestampCount);
        WireProtocol.validateRowValueCount(deleteTimestampCount);

        List<Long> writeTimestamps = readTimestamps(writeTimestampCount);
        List<Long> deleteTimestamps = readTimestamps(deleteTimestampCount);
        List<UnversionedValue> keys = readSchemafulValues(schemaData, keyCount);
        List<VersionedValue> values = readVersionedValues(valueCount);
        return new VersionedRow(writeTimestamps, deleteTimestamps, keys, values);
    }

    private int readRowCount() {
        return WireProtocol.validateRowCount(readLong());
    }

    public List<UnversionedRow> readSchemafulRowset(List<WireColumnSchema> schemaData) {
        int rowCount = readRowCount();
        List<UnversionedRow> rows = new ArrayList<>(rowCount);
        for (int i = 0; i < rowCount; i++) {
            rows.add(readSchemafulRow(schemaData));
        }
        return rows;
    }

    public List<UnversionedRow> readUnversionedRowset() {
        int rowCount = readRowCount();
        List<UnversionedRow> rows = new ArrayList<>(rowCount);
        for (int i = 0; i < rowCount; i++) {
            rows.add(readUnversionedRow());
        }
        return rows;
    }

    public List<VersionedRow> readVersionedRowset(List<WireColumnSchema> schemaData) {
        int rowCount = readRowCount();
        List<VersionedRow> rows = new ArrayList<>(rowCount);
        for (int i = 0; i < rowCount; i++) {
            rows.add(readVersionedRow(schemaData));
        }
        return rows;
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
