package tech.ytsaurus.flow.internal.row;

import java.nio.charset.StandardCharsets;

import com.google.protobuf.MessageLite;
import org.jspecify.annotations.Nullable;
import tech.ytsaurus.client.DefaultSerializationResolver;
import tech.ytsaurus.client.rows.UnversionedValue;
import tech.ytsaurus.core.tables.ColumnValueType;
import tech.ytsaurus.flow.row.codec.CodecRegistry;
import tech.ytsaurus.flow.utils.ProtoUtils;

/**
 * Utility class for converting between {@link UnversionedValue} and particular Java types.
 */
public final class UnversionedValueHelper {

    private UnversionedValueHelper() {
    }

    /**
     * Narrows a UINT64 value (stored as a signed {@code long}) to {@code int}, range-checking against
     * the unsigned 32-bit range {@code [0, 0xFFFFFFFF]} rather than the signed range used by
     * {@link Math#toIntExact(long)}. A UINT64 with the high bit set is a valid unsigned value held as a
     * negative {@code long}, so signed {@code toIntExact} would wrongly reject it.
     *
     * @param value UINT64 value held in a {@code long}.
     * @return the value narrowed to {@code int} (same 32-bit pattern).
     * @throws ArithmeticException if the value does not fit in unsigned 32 bits.
     */
    static int uint64ToInt(long value) {
        if ((value & ~0xFFFFFFFFL) != 0) {
            throw new ArithmeticException("UINT64 value out of int range: " + Long.toUnsignedString(value));
        }
        return (int) value;
    }

    /**
     * Performs conversion from {@link UnversionedValue} to particular Java type.
     *
     * @param value     UnversionedValue to be converted.
     * @param fieldType Java Class to be converted to.
     * @return Converted value.
     * @throws IllegalArgumentException for unknown or unsupported types.
     */
    @SuppressWarnings("unchecked")
    public static @Nullable Object convertValueFrom(
            UnversionedValue value,
            Class<?> fieldType
    ) {
        ColumnValueType type = value.getType();
        Object rawValue = value.getValue();
        switch (type) {
            case NULL:
                return null;
            case INT64:
                if (rawValue == null || fieldType == Long.class || fieldType == long.class) {
                    return rawValue;
                }
                if (fieldType == Integer.class || fieldType == int.class) {
                    return Math.toIntExact((Long) rawValue);
                }
                break;
            case UINT64:
                if (rawValue == null || fieldType == Long.class || fieldType == long.class) {
                    return rawValue;
                }
                if (fieldType == Integer.class || fieldType == int.class) {
                    return uint64ToInt((Long) rawValue);
                }
                break;
            case DOUBLE:
                if (rawValue == null || fieldType == Double.class || fieldType == double.class) {
                    return (Double) rawValue;
                }
                break;
            case BOOLEAN:
                if (rawValue == null || fieldType == Boolean.class || fieldType == boolean.class) {
                    return (Boolean) rawValue;
                }
                break;
            case STRING:
            case ANY:
            case COMPOSITE:
                if (rawValue == null) {
                    return null;
                }
                if (fieldType == byte[].class) {
                    return value.bytesValue();
                }
                if (fieldType == String.class) {
                    return value.stringValue();
                }
                // Protobuf.
                if (MessageLite.class.isAssignableFrom(fieldType)) {
                    return ProtoUtils.parseBytes(
                            value.bytesValue(),
                            (Class<? extends MessageLite>) fieldType
                    );
                }
                // Consider everything else as binary YSON.
                return CodecRegistry.getInstance().getYsonCodec().decode(value.bytesValue(), fieldType);
            default:
                throw new IllegalArgumentException("Unexpected type " + type);
        }
        throw new IllegalArgumentException("Cannot convert " + type + " to " + fieldType);
    }

    /**
     * Performs conversion of provided object into a value acceptable by
     * {@link UnversionedValue} for the supplied {@link ColumnValueType}.
     *
     * @param value Java value to convert (may be {@code null}).
     * @param type  target column value type.
     * @return value acceptable by {@code UnversionedValue}.
     */
    public static <T> @Nullable Object convertValueTo(@Nullable T value, ColumnValueType type) {
        return switch (type) {
            case STRING, ANY, COMPOSITE -> switch (value) {
                case null -> null;
                case byte[] ignored -> value;
                case String s -> s.getBytes(StandardCharsets.UTF_8);
                case MessageLite messageLite -> messageLite.toByteArray();
                default -> CodecRegistry.getInstance().getYsonCodec().encode(value);
            };
            default -> UnversionedValue.convertValueTo(value, type, DefaultSerializationResolver.getInstance());
        };
    }
}
