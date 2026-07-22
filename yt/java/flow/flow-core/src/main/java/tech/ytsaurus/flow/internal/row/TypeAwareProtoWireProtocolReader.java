package tech.ytsaurus.flow.internal.row;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;

import com.google.protobuf.ByteString;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.MessageLite;
import org.jspecify.annotations.Nullable;
import tech.ytsaurus.core.rows.YTreeSerializer;
import tech.ytsaurus.core.tables.ColumnValueType;
import tech.ytsaurus.flow.row.codec.CodecRegistry;
import tech.ytsaurus.flow.row.codec.YsonCodec;
import tech.ytsaurus.flow.typeinfo.TypeInfo;
import tech.ytsaurus.flow.utils.ProtoUtils;

/**
 * Reader that deserializes a chunk in {@code UnversionedRow} binary layout into a typed
 * entity using cached per-column type information.
 *
 * @param <Entity> entity type to deserialize into
 */
public class TypeAwareProtoWireProtocolReader<Entity> {
    private final TypeInfo<Entity> typeInfo;
    private final YsonCodec ysonCodec;
    // Cached metadata for performance - avoids repeated getter calls in hot path
    private final TypeInfo.Column[] columns;
    private final Class<?>[] fieldTypes;
    private final boolean[] isProtobuf;
    private final @Nullable YTreeSerializer<?>[] yTreeSerializers;

    /**
     * Creates a reader by introspecting {@code entityClass} for its column layout.
     *
     * @param entityClass class of the entity to be deserialized
     */
    public TypeAwareProtoWireProtocolReader(Class<Entity> entityClass) {
        this(new TypeInfo<>(entityClass));
    }

    /**
     * Creates a reader bound to a pre-built {@link TypeInfo}.
     *
     * @param typeInfo type information for the target entity
     */
    public TypeAwareProtoWireProtocolReader(TypeInfo<Entity> typeInfo) {
        this.typeInfo = typeInfo;
        this.ysonCodec = CodecRegistry.getInstance().getYsonCodec();

        // Pre-compute and cache column metadata
        int columnCount = typeInfo.getColumns().size();
        this.fieldTypes = new Class<?>[columnCount];
        this.isProtobuf = new boolean[columnCount];
        this.yTreeSerializers = new YTreeSerializer<?>[columnCount];
        this.columns = typeInfo.getColumns().toArray(new TypeInfo.Column[0]);

        for (int i = 0; i < columnCount; i++) {
            var column = typeInfo.getColumns().get(i);
            this.fieldTypes[i] = column.getDescriptor().getField().getType();
            this.isProtobuf[i] = column.getDescriptor().isProtobuf();
            this.yTreeSerializers[i] = column.getDescriptor().getYTreeSerializer();
        }
    }

    /**
     * Reads a chunk of data and deserializes it into an {@link Entity} instance according to the type information.
     * The chunk must have the UnversionedRow binary layout.
     *
     * @param chunk The ByteString representing the serialized row.
     * @return The deserialized {@link Entity} instance.
     * @throws UncheckedIOException  If an I/O error occurs during reading.
     * @throws IllegalStateException If the version is unsupported or column IDs do not match to schema.
     */
    @SuppressWarnings("unchecked")
    public Entity readChunk(ByteString chunk) {
        final CodedInputStream chunkStream = chunk.newCodedInput();
        var instance = typeInfo.createInstance();
        try {
            final int version = chunkStream.readUInt32();
            if (version != 0) {
                throw new IllegalStateException("Unversioned row does not support versions.");
            }
            final int valueCount0 = chunkStream.readUInt32();
            if (valueCount0 == -1) {
                // No values, return empty instance.
                return instance;
            }
            for (int i = 0; i < columns.length; i++) {
                var column = columns[i];
                final int id = chunkStream.readUInt32() & 0xffff;
                if (id != i) {
                    throw new IllegalStateException("Unexpected column id (Got: %d, Expected: %d)".formatted(id, i));
                }
                final ColumnValueType type = ColumnValueType.fromValue(chunkStream.readRawByte() & 0xff);
                Object value = null;
                Class<?> fieldType = fieldTypes[i];
                switch (type) {
                    case NULL:
                        break;
                    case INT64:
                        long longValue = chunkStream.readSInt64();
                        if (fieldType == Long.class || fieldType == long.class) {
                            value = longValue;
                            break;
                        }
                        if (fieldType == Integer.class || fieldType == int.class) {
                            value = Math.toIntExact(longValue);
                            break;
                        }
                        if (fieldType == Short.class || fieldType == short.class) {
                            value = ((Long) longValue).shortValue();
                            break;
                        }
                        if (fieldType == Byte.class || fieldType == byte.class) {
                            value = ((Long) longValue).byteValue();
                            break;
                        }
                        break;
                    case UINT64:
                        long unsignedValue = chunkStream.readUInt64();
                        if (fieldType == Long.class || fieldType == long.class) {
                            value = unsignedValue;
                            break;
                        }
                        if (fieldType == Integer.class || fieldType == int.class) {
                            value = UnversionedValueHelper.uint64ToInt(unsignedValue);
                            break;
                        }
                        if (fieldType == Short.class || fieldType == short.class) {
                            value = ((Long) unsignedValue).shortValue();
                            break;
                        }
                        if (fieldType == Byte.class || fieldType == byte.class) {
                            value = ((Long) unsignedValue).byteValue();
                            break;
                        }
                        break;

                    case DOUBLE:
                        // Fixed 8-byte little-endian values, not varint.
                        double doubleValue = Double.longBitsToDouble(chunkStream.readFixed64());
                        if (fieldType == Double.class || fieldType == double.class) {
                            value = doubleValue;
                            break;
                        }
                        break;

                    case BOOLEAN:
                        boolean booleanValue = chunkStream.readUInt64() != 0;
                        if (fieldType == Boolean.class || fieldType == boolean.class) {
                            value = booleanValue;
                            break;
                        }
                        break;

                    case STRING:
                    case ANY:
                    case COMPOSITE:
                        long size = chunkStream.readUInt64();
                        if (size < 0 || size > Integer.MAX_VALUE) {
                            throw new IllegalStateException("Unsupported " + type + " data length " + size);
                        }
                        byte[] rawBytes = chunkStream.readRawBytes((int) size);
                        if (fieldType == String.class) {
                            value = new String(rawBytes, StandardCharsets.UTF_8);
                            break;
                        }
                        if (fieldType == byte[].class) {
                            value = rawBytes;
                            break;
                        }
                        // Protobuf
                        if (isProtobuf[i]) {
                            value = ProtoUtils.parseBytes(
                                    rawBytes,
                                    (Class<? extends MessageLite>) fieldType
                            );
                            break;
                        }
                        // Binary YSON (pluggable via YsonCodec)
                        value = ysonCodec.decodeWithSerializer(yTreeSerializers[i], rawBytes, fieldType);
                        break;
                    default:
                        throw new IllegalArgumentException("Unexpected type " + type);
                }
                column.set(instance, value);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to read input chunk", e);
        }
        return instance;
    }
}
