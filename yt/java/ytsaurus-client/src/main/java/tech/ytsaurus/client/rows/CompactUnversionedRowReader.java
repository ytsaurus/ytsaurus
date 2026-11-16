package tech.ytsaurus.client.rows;

import java.io.IOException;

import javax.annotation.Nullable;

import com.google.protobuf.ByteString;
import com.google.protobuf.CodedInputStream;
import tech.ytsaurus.core.tables.ColumnValueType;

/**
 * Reads the compact format produced by C++ {@code SerializeToString(TUnversionedRow)}.
 */
public final class CompactUnversionedRowReader {
    private static final int FORMAT_VERSION = 0;

    private final CodedInputStream input;
    private final int inputSize;

    public CompactUnversionedRowReader(byte[] data) {
        input = CodedInputStream.newInstance(data);
        inputSize = data.length;
    }

    public CompactUnversionedRowReader(ByteString data) {
        input = data.newCodedInput();
        inputSize = data.size();
    }

    @Nullable
    public UnversionedRow readUnversionedRow() {
        return readUnversionedRow(new UnversionedRowDeserializer());
    }

    @Nullable
    public <T> T readUnversionedRow(WireRowDeserializer<T> deserializer) {
        try {
            if (input.isAtEnd()) {
                return deserializer.onNullRow();
            }
            int version = input.readUInt32();
            if (version != FORMAT_VERSION) {
                throw new IllegalArgumentException("Unsupported compact unversioned row format version " +
                        Integer.toUnsignedString(version));
            }
            int valueCount = input.readUInt32();
            // Each value needs at least an id and a type byte.
            if (valueCount < 0 || valueCount > (inputSize - input.getTotalBytesRead()) / 2) {
                throw new IllegalArgumentException("Invalid compact unversioned row value count " +
                        Integer.toUnsignedString(valueCount));
            }
            WireValueDeserializer<?> valueDeserializer = deserializer.onNewRow(valueCount);
            for (int index = 0; index < valueCount; ++index) {
                readValue(valueDeserializer);
            }
            return deserializer.onCompleteRow();
        } catch (IOException e) {
            throw new IllegalArgumentException("Malformed compact unversioned row", e);
        }
    }

    private void readValue(WireValueDeserializer<?> deserializer) throws IOException {
        int id = input.readUInt32();
        ColumnValueType type = ColumnValueType.fromValue(input.readUInt32());
        deserializer.setId(id);
        deserializer.setType(type);
        deserializer.setAggregate(false);
        deserializer.setTimestamp(-1);
        switch (type) {
            case NULL:
            case MIN:
            case MAX:
            case THE_BOTTOM:
                deserializer.onEntity();
                break;
            case INT64:
                deserializer.onInteger(input.readSInt64());
                break;
            case UINT64:
                deserializer.onInteger(input.readUInt64());
                break;
            case DOUBLE:
                deserializer.onDouble(Double.longBitsToDouble(input.readRawLittleEndian64()));
                break;
            case BOOLEAN:
                deserializer.onBoolean(input.readRawByte() == 1);
                break;
            case STRING:
            case ANY:
            case COMPOSITE:
                deserializer.onBytes(input.readRawBytes(input.readUInt32()));
                break;
            default:
                throw new IllegalArgumentException("Unexpected value type " + type);
        }
        deserializer.build();
    }
}
