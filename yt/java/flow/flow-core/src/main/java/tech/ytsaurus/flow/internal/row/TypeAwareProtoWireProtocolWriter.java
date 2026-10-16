package tech.ytsaurus.flow.internal.row;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;

import com.google.protobuf.ByteString;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.MessageLite;
import com.google.protobuf.UnsafeByteOperations;
import org.jspecify.annotations.Nullable;
import tech.ytsaurus.client.rows.WireProtocol;
import tech.ytsaurus.core.rows.YTreeSerializer;
import tech.ytsaurus.core.tables.ColumnValueType;
import tech.ytsaurus.flow.internal.utils.ExposedByteArrayOutputStream;
import tech.ytsaurus.flow.row.codec.CodecRegistry;
import tech.ytsaurus.flow.row.codec.YsonCodec;
import tech.ytsaurus.flow.typeinfo.TypeInfo;

/**
 * Writer that serializes typed entities into the {@code UnversionedRow} binary layout.
 *
 * @param <Entity> entity type to serialize
 */
public class TypeAwareProtoWireProtocolWriter<Entity> {
    // TODO (sergeypozdeev): make buffer configurable.
    private static final int DEFAULT_BUFFER_SIZE = 2048; // 2kb.

    private final TypeInfo<Entity> typeInfo;
    private final YsonCodec ysonCodec;
    private final @Nullable YTreeSerializer<?>[] yTreeSerializers;

    /**
     * Creates a writer by introspecting {@code entityClass} for its column layout.
     *
     * @param entityClass class of the entity to be serialized
     */
    public TypeAwareProtoWireProtocolWriter(Class<Entity> entityClass) {
        this(new TypeInfo<>(entityClass));
    }

    /**
     * Creates a writer bound to a pre-built {@link TypeInfo}.
     *
     * @param typeInfo type information for the source entity
     */
    public TypeAwareProtoWireProtocolWriter(TypeInfo<Entity> typeInfo) {
        this.typeInfo = typeInfo;
        this.ysonCodec = CodecRegistry.getInstance().getYsonCodec();
        int columnCount = typeInfo.getColumns().size();
        this.yTreeSerializers = new YTreeSerializer<?>[columnCount];
        for (int i = 0; i < columnCount; i++) {
            this.yTreeSerializers[i] = typeInfo.getColumns().get(i).getDescriptor().getYTreeSerializer();
        }
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

    private static void incorrectTypeException(
            Class<?> fieldType,
            ColumnValueType columnValueType
    ) throws IllegalArgumentException {
        throw new IllegalArgumentException(
                "Mismatch between fieldType '%s' and columnValueType '%s'".formatted(fieldType, columnValueType)
        );
    }

    /**
     * Serializes an entity into the {@code UnversionedRow} binary layout.
     *
     * @param entity the entity to serialize
     * @return the serialized bytes
     * @throws UncheckedIOException if an I/O error occurs during serialization
     */
    public ByteString writeEntity(Entity entity) {
        try {
            var os = new ExposedByteArrayOutputStream(DEFAULT_BUFFER_SIZE);
            final CodedOutputStream chunkStream = CodedOutputStream.newInstance(os);
            writeRowHeader(chunkStream);
            for (int i = 0; i < typeInfo.getColumns().size(); i++) {
                var column = typeInfo.getColumns().get(i);
                writeValue(i, entity, column, chunkStream);
            }
            chunkStream.flush();
            // Wrap the backing buffer directly to avoid the extra copy made by toByteArray().
            return UnsafeByteOperations.unsafeWrap(os.buffer(), 0, os.length());
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to write entity to byte array", e);
        }
    }

    private void writeRowHeader(CodedOutputStream chunkStream) throws IOException {
        // Version is always 0 for unversioned row.
        chunkStream.writeUInt32NoTag(0);
        // Size.
        chunkStream.writeUInt32NoTag(typeInfo.getColumns().size());
    }

    private void writeValue(
            int valueId,
            Entity entity,
            TypeInfo.Column column,
            CodedOutputStream chunkStream
    ) throws IOException {
        // Extract raw column value.
        Object rawValue = column.get(entity);
        // Write header.
        chunkStream.writeUInt64NoTag(WireProtocol.validateColumnId(valueId)); //columnId.
        if (rawValue == null) {
            // Write NULL marker and return.
            chunkStream.writeRawByte((byte) (ColumnValueType.NULL.getValue() & 0xFF)); // null type
            return;
        }
        chunkStream.writeRawByte((byte) (column.getSchema().getType().getValue() & 0xFF)); //type

        // Write value.
        Class<?> fieldType = column.getDescriptor().getField().getType();

        switch (column.getSchema().getType()) {
            case INT64:
                if (fieldType == Long.class || fieldType == long.class) {
                    chunkStream.writeSInt64NoTag((long) rawValue);
                    break;
                }
                if (fieldType == Integer.class || fieldType == int.class) {
                    chunkStream.writeSInt64NoTag((int) rawValue);
                    break;
                }
                if (fieldType == Short.class || fieldType == short.class) {
                    chunkStream.writeSInt64NoTag((short) rawValue);
                    break;
                }
                if (fieldType == Byte.class || fieldType == byte.class) {
                    chunkStream.writeSInt64NoTag((byte) rawValue);
                    break;
                }
                incorrectTypeException(fieldType, column.getSchema().getType());
            case UINT64:
                if (fieldType == Long.class || fieldType == long.class) {
                    chunkStream.writeUInt64NoTag((long) rawValue);
                    break;
                }
                if (fieldType == Integer.class || fieldType == int.class) {
                    chunkStream.writeUInt64NoTag(Integer.toUnsignedLong((int) rawValue));
                    break;
                }
                if (fieldType == Short.class || fieldType == short.class) {
                    chunkStream.writeUInt64NoTag(Short.toUnsignedLong((short) rawValue));
                    break;
                }
                if (fieldType == Byte.class || fieldType == byte.class) {
                    chunkStream.writeUInt64NoTag(Byte.toUnsignedLong((byte) rawValue));
                    break;
                }
                incorrectTypeException(fieldType, column.getSchema().getType());
            case DOUBLE:
                // Fixed 8-byte little-endian values, not varint.
                chunkStream.writeFixed64NoTag(Double.doubleToRawLongBits((Double) rawValue));
                break;
            case BOOLEAN:
                chunkStream.writeUInt64NoTag(((Boolean) rawValue) ? 1L : 0L);
                break;
            case STRING:
            case ANY:
            case COMPOSITE:
                ColumnValueType stringLikeType = column.getSchema().getType();
                if (fieldType == String.class) {
                    byte[] data = ((String) rawValue).getBytes(StandardCharsets.UTF_8);
                    validateStringLikeValueLength(stringLikeType, data.length);
                    chunkStream.writeUInt64NoTag(data.length);
                    chunkStream.writeRawBytes(data);
                    break;
                }
                // If byte[], just cast.
                if (fieldType == byte[].class) {
                    byte[] bytes = (byte[]) rawValue;
                    validateStringLikeValueLength(stringLikeType, bytes.length);
                    chunkStream.writeUInt64NoTag(bytes.length);
                    chunkStream.writeRawBytes(bytes);
                    break;
                }
                if (column.getDescriptor().isProtobuf()) {
                    MessageLite message = (MessageLite) rawValue;
                    validateStringLikeValueLength(stringLikeType, message.getSerializedSize());
                    chunkStream.writeUInt64NoTag(message.getSerializedSize());
                    message.writeTo(chunkStream);
                    break;
                }
                // Consider everything else as binary YSON (pluggable via YsonCodec).
                byte[] ysonBytes = ysonCodec.encodeWithSerializer(yTreeSerializers[valueId], rawValue);
                validateStringLikeValueLength(stringLikeType, ysonBytes.length);
                chunkStream.writeUInt64NoTag(ysonBytes.length);
                chunkStream.writeRawBytes(ysonBytes);
                break;
            default:
                break;
        }
    }
}
