package tech.ytsaurus.flow.internal.row;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;

import com.google.protobuf.ByteString;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.UnsafeByteOperations;
import tech.ytsaurus.client.rows.UnversionedRow;
import tech.ytsaurus.client.rows.UnversionedValue;
import tech.ytsaurus.client.rows.WireProtocol;
import tech.ytsaurus.core.tables.ColumnValueType;

/**
 * Protobuf-friendly version of {@link tech.ytsaurus.client.rows.WireProtocolWriter}
 * <p>
 * Class is lightweight, it is supposed to create a new instance for each UnversionedRow.
 */
public class ProtoWireProtocolWriter {

    private final byte[] internalDataChunk;
    private final CodedOutputStream chunkStream;

    /**
     * Construct ProtoWireProtocolWriter instance for provided UnversionedRow.
     *
     * @param row UnversionedRow to be serialized into {@link ByteString}.
     */
    public ProtoWireProtocolWriter(UnversionedRow row) {
        int estimatedSize = estimateRowSize(row);
        this.internalDataChunk = new byte[estimatedSize];
        this.chunkStream = CodedOutputStream.newInstance(internalDataChunk);
        writeUnversionedRow(row);
    }

    static int estimateRowSize(UnversionedRow row) {
        int size = Integer.BYTES * 2; // header
        for (UnversionedValue value : row.getValues()) {
            size += estimateRowValueSize(value);
        }
        return size;
    }

    static int estimateRowValueSize(UnversionedValue value) {
        int size = Integer.BYTES * 2; // id and type
        switch (value.getType()) {
            case NULL:
            case MIN:
            case MAX:
            case THE_BOTTOM:
                break;
            case INT64:
            case UINT64:
                size += Long.BYTES;
                break;
            case DOUBLE:
                size += Double.BYTES;
                break;
            case BOOLEAN:
                size += 1;
                break;
            case STRING:
            case ANY:
            case COMPOSITE:
                size += Integer.BYTES + value.getLength();
                break;
        }
        return size;
    }

    /**
     * @return ByteString from serialized UnversionedRow.
     */
    public ByteString getBinary() {
        return UnsafeByteOperations.unsafeWrap(
                internalDataChunk,
                0,
                chunkStream.getTotalBytesWritten()
        );
    }

    private static void validateStringLikeValueLength(ColumnValueType type, int length) {
        int limit;
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
                return;
        }
        if (length > limit) {
            throw new IllegalStateException("Unsupported " + type + " data length " + length);
        }
    }

    private void writeUnversionedValue(UnversionedValue value) throws IOException {
        switch (value.getType()) {
            case INT64:
                chunkStream.writeSInt64NoTag(value.longValue());
                break;
            case UINT64:
            case BOOLEAN:
                chunkStream.writeUInt64NoTag(value.toRawBits());
                break;
            case DOUBLE:
                // Fixed 8-byte little-endian values, not varint.
                chunkStream.writeFixed64NoTag(value.toRawBits());
                break;
            case STRING:
            case ANY:
            case COMPOSITE:
                byte[] data = value.bytesValue();
                validateStringLikeValueLength(value.getType(), data.length);
                chunkStream.writeUInt64NoTag(data.length);
                chunkStream.writeRawBytes(data);
                break;
            default:
                break;
        }
    }

    private void writeUnversionedValues(List<UnversionedValue> values) throws IOException {
        for (UnversionedValue value : values) {
            writeUnversionedValueHeader(value);
            writeUnversionedValue(value);
        }
    }

    private void writeUnversionedValueHeader(UnversionedValue value) throws IOException {
        chunkStream.writeUInt64NoTag(WireProtocol.validateColumnId(value.getId())); //columnId
        chunkStream.writeRawByte((byte) (value.getType().getValue() & 0xFF)); //type
    }

    private void writeUnversionedRowHeader(UnversionedRow row) throws IOException {
        //Version is always 0 for unversioned row
        chunkStream.writeUInt32NoTag(0);
        //Size
        chunkStream.writeUInt32NoTag(row.getValues().size());
    }

    private void writeUnversionedRow(UnversionedRow row) {
        try {
            writeUnversionedRowHeader(row);
            writeUnversionedValues(row.getValues());
            chunkStream.flush();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
