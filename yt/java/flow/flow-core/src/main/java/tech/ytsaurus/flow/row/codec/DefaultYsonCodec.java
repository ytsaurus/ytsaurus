package tech.ytsaurus.flow.row.codec;

import java.io.ByteArrayOutputStream;
import java.lang.reflect.Type;

import tech.ytsaurus.core.rows.YTreeSerializer;
import tech.ytsaurus.core.utils.ClassUtils;
import tech.ytsaurus.flow.utils.YsonUtils;
import tech.ytsaurus.ysontree.YTreeBinarySerializer;

/**
 * Default YSON (de)serialization codec. Stateless and thread-safe.
 */
public final class DefaultYsonCodec implements YsonCodec {

    /**
     * Shared stateless instance.
     */
    public static final DefaultYsonCodec INSTANCE = new DefaultYsonCodec();

    private DefaultYsonCodec() {
    }

    /**
     * Resolves a YSON serializer for {@code type}, using a shared cache.
     *
     * @param type runtime type of the value
     * @return the resolved serializer
     * @throws IllegalArgumentException if {@code type} is unsupported
     */
    @Override
    public YTreeSerializer<?> getOrCreateSerializer(Type type) {
        return CacheAwareYTreeSerializerFactory.INSTANCE.forType(type);
    }

    /**
     * Encodes {@code value} to YSON binary form using the supplied {@code serializer}.
     *
     * @param serializer serializer to apply to {@code value}
     * @param value      value to encode
     * @return YSON-encoded bytes
     * @throws IllegalArgumentException if {@code serializer} is {@code null}
     */
    @SuppressWarnings("unchecked")
    @Override
    public byte[] encodeWithSerializer(YTreeSerializer<?> serializer, Object value) {
        var typedSerializer = (YTreeSerializer<Object>) serializer;
        var baos = new ByteArrayOutputStream();
        var ysonConsumer = YTreeBinarySerializer.getSerializer(baos);
        typedSerializer.serialize(ClassUtils.castToType(value), ysonConsumer);
        ysonConsumer.close();
        return baos.toByteArray();
    }

    /**
     * Decodes YSON binary bytes into a value using the supplied {@code serializer}.
     *
     * @param serializer serializer to apply to {@code bytes}
     * @param bytes      YSON-encoded bytes
     * @param fieldType  target Java type (used only for diagnostics)
     * @return the decoded value
     * @throws IllegalArgumentException if {@code serializer} is {@code null}
     */
    @Override
    public Object decodeWithSerializer(
            YTreeSerializer<?> serializer,
            byte[] bytes,
            Class<?> fieldType
    ) {
        var yTree = YsonUtils.yTreeFromBinary(bytes);
        return serializer.deserialize(yTree);
    }

    /**
     * Encodes {@code value} to YSON binary form by resolving a serializer for its runtime class
     * through {@link CacheAwareYTreeSerializerFactory}.
     *
     * @param value value to encode
     * @return YSON-encoded bytes
     * @throws IllegalArgumentException if no serializer is available for {@code value}
     */
    @Override
    public byte[] encode(Object value) {
        var serializer = CacheAwareYTreeSerializerFactory.INSTANCE.forType(value.getClass());
        return encodeWithSerializer(serializer, value);
    }

    /**
     * Decodes YSON binary bytes into a value of {@code fieldType} by resolving a serializer
     * through {@link CacheAwareYTreeSerializerFactory}.
     *
     * @param bytes     YSON-encoded bytes
     * @param fieldType target Java type
     * @return the decoded value
     * @throws IllegalArgumentException if no serializer is available for {@code fieldType}
     */
    @Override
    public Object decode(byte[] bytes, Class<?> fieldType) {
        var serializer = CacheAwareYTreeSerializerFactory.INSTANCE.forType(fieldType);
        return decodeWithSerializer(serializer, bytes, fieldType);
    }
}
