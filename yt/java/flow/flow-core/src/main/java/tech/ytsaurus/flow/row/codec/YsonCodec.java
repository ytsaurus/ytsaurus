package tech.ytsaurus.flow.row.codec;

import java.lang.reflect.Type;

import tech.ytsaurus.core.rows.YTreeSerializer;

/**
 * Codec for (de)serializing YSON-encoded column values.
 *
 * <p>Provides a serializer-based API for hot paths and a handle-less API for
 * dynamic call-sites. Implementations must be stateless and thread-safe.
 */
public interface YsonCodec {

    /**
     * Returns a {@link YTreeSerializer} for the given type.
     * <p>
     * The implementation can either create a new instance or return an existing one.
     *
     * @param type runtime type of the column value; for parameterized fields pass
     *             {@code field.getGenericType()} so that container element types are preserved
     * @return serializer for {@code type}
     * @throws IllegalArgumentException if {@code type} is unsupported
     */
    YTreeSerializer<?> getOrCreateSerializer(Type type);

    /**
     * Encodes {@code value} to YSON using a pre-computed serializer.
     *
     * @param serializer serializer previously returned by {@link #getOrCreateSerializer}
     * @param value      value to encode
     * @return YSON-encoded bytes
     * @throws IllegalArgumentException if {@code serializer} is {@code null}
     */
    byte[] encodeWithSerializer(YTreeSerializer<?> serializer, Object value);

    /**
     * Decodes YSON {@code bytes} to {@code fieldType} using a pre-computed serializer.
     *
     * @param serializer serializer previously returned by {@link #getOrCreateSerializer}
     * @param bytes      YSON-encoded bytes
     * @param fieldType  target Java type
     * @return decoded value
     * @throws IllegalArgumentException if {@code serializer} is {@code null}
     */
    Object decodeWithSerializer(
            YTreeSerializer<?> serializer,
            byte[] bytes,
            Class<?> fieldType
    );

    /**
     * Encodes {@code value} to YSON.
     *
     * @param value value to encode
     * @return YSON-encoded bytes
     */
    byte[] encode(Object value);

    /**
     * Decodes YSON {@code bytes} to {@code fieldType}.
     *
     * @param bytes     YSON-encoded bytes
     * @param fieldType target Java type
     * @return decoded value
     */
    Object decode(byte[] bytes, Class<?> fieldType);
}
