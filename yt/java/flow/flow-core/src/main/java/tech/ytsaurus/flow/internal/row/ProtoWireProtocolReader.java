package tech.ytsaurus.flow.internal.row;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.function.LongSupplier;

import com.google.protobuf.ByteString;
import com.google.protobuf.CodedInputStream;
import org.jspecify.annotations.Nullable;
import tech.ytsaurus.client.rows.UnversionedRow;
import tech.ytsaurus.client.rows.UnversionedRowDeserializer;
import tech.ytsaurus.client.rows.WireProtocol;
import tech.ytsaurus.client.rows.WireRowDeserializer;
import tech.ytsaurus.client.rows.WireValueDeserializer;
import tech.ytsaurus.core.tables.ColumnValueType;

/**
 * Protobuf-friendly version of {@link tech.ytsaurus.client.rows.WireProtocolReader}
 * <p>
 * Class is lightweight, it is supposed to create a new instance for each chunk of data.
 */
public class ProtoWireProtocolReader {

    private final CodedInputStream chunkStream;

    /**
     * Construct ProtoWireProtocolReader instance for provided data chunk.
     *
     * @param chunk byte array to be deserialized into UnversionedRow.
     */
    public ProtoWireProtocolReader(byte[] chunk) {
        this.chunkStream = CodedInputStream.newInstance(chunk);
    }

    /**
     * Construct ProtoWireProtocolReader instance for provided {@link ByteString} data chunk.
     * <p>
     * This constructor avoids the extra byte array copy that would occur with
     * {@code new ProtoWireProtocolReader(byteString.toByteArray())}.
     * </p>
     *
     * @param chunk ByteString to be deserialized into UnversionedRow.
     */
    public ProtoWireProtocolReader(ByteString chunk) {
        this.chunkStream = chunk.newCodedInput();
    }

    private void readUnversionedValues(WireValueDeserializer<?> consumer, int valueCount) throws IOException {
        for (int i = 0; i < valueCount; i++) {
            readValue(() -> -1, consumer);
        }
    }

    private <T> void readValueImpl(WireValueDeserializer<T> consumer, ColumnValueType type) throws IOException {
        consumer.setType(type);
        if (type.isStringLikeType()) {
            long size = chunkStream.readUInt64();
            if (size < 0 || size > Integer.MAX_VALUE) {
                throw new IllegalStateException("Unsupported " + type + " data length " + size);
            }
            final byte[] bytes = chunkStream.readRawBytes((int) size);
            consumer.onBytes(bytes);
        } else if (type.isValueType()) {
            switch (type) {
                case INT64:
                    consumer.onInteger(chunkStream.readSInt64());
                    break;
                case UINT64:
                    consumer.onInteger(chunkStream.readUInt64());
                    break;
                case DOUBLE:
                    // Fixed 8-byte little-endian values, not varint.
                    consumer.onDouble(Double.longBitsToDouble(chunkStream.readFixed64()));
                    break;
                case BOOLEAN:
                    consumer.onBoolean(chunkStream.readBool());
                    break;
                default:
                    throw new IllegalArgumentException(type + " cannot be represented as raw bits");
            }
        } else {
            consumer.onEntity(); // no value
        }
    }

    private <T> void readValue(LongSupplier timestampSupplier, WireValueDeserializer<T> consumer) throws IOException {
        final int id = chunkStream.readUInt32() & 0xffff;
        consumer.setId(id);
        final ColumnValueType type = ColumnValueType.fromValue(chunkStream.readRawByte() & 0xff);
        this.readValueImpl(consumer, type);
        consumer.setTimestamp(timestampSupplier.getAsLong());
        consumer.build();
    }

    /**
     * Read chunk using provided {@link WireRowDeserializer}.
     *
     * @param deserializer Deserializer.
     * @param <T>          Type parameter from WireRowDeserializer&lt;T&gt;.
     * @return Row.
     */
    public <T> @Nullable T readUnversionedRow(WireRowDeserializer<T> deserializer) {
        try {
            final int version = chunkStream.readUInt32();
            if (version != 0) {
                throw new IllegalStateException("Unversioned row does not support versions.");
            }
            final int valueCount0 = chunkStream.readUInt32();
            if (valueCount0 == -1) {
                return deserializer.onNullRow();
            }
            final int valueCount = WireProtocol.validateRowValueCount(valueCount0);
            final WireValueDeserializer<?> consumer = deserializer.onNewRow(valueCount);
            readUnversionedValues(consumer, valueCount);
            return deserializer.onCompleteRow();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to read input byte array", e);
        }
    }

    /**
     * Read chunk into UnversionedRow using default {@link UnversionedRowDeserializer}.
     *
     * @return UnversionedRow.
     * @see #readUnversionedRow(WireRowDeserializer)
     */
    public @Nullable UnversionedRow readUnversionedRow() {
        try {
            var deserializer = new UnversionedRowDeserializer();
            final int version = chunkStream.readUInt32();
            if (version != 0) {
                throw new IllegalStateException("Unversioned row does not support versions.");
            }
            final int valueCount0 = chunkStream.readUInt32();
            if (valueCount0 == -1) {
                return deserializer.onNullRow();
            }
            final int valueCount = WireProtocol.validateRowValueCount(valueCount0);
            final WireValueDeserializer<?> consumer = deserializer.onNewRow(valueCount);
            readUnversionedValues(consumer, valueCount);
            return deserializer.onCompleteRow();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to read input byte array", e);
        }
    }
}
