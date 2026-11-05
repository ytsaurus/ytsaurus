package tech.ytsaurus.flow.utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import com.google.protobuf.ByteString;
import org.jspecify.annotations.Nullable;
import tech.ytsaurus.flow.row.codec.DefaultYsonCodec;
import tech.ytsaurus.ysontree.YTreeBinarySerializer;
import tech.ytsaurus.ysontree.YTreeNode;

/**
 * Helper class for interaction with YSON messages.
 */
public class YsonUtils {

    private YsonUtils() {
    }

    /**
     * Convert ByteString to YTreeNode.
     *
     * @param protoYson ByteString.
     * @return YTreeNode.
     */
    public static YTreeNode yTreeFromProto(ByteString protoYson) {
        return YTreeBinarySerializer.deserialize(protoYson.newInput());
    }

    /**
     * Convert YTreeNode to ByteString.
     *
     * @param yTreeNode YTreeNode.
     * @return ByteString.
     */
    public static ByteString protoFromYTree(YTreeNode yTreeNode) {
        return ByteString.copyFrom(serializeYTree(yTreeNode));
    }

    /**
     * Convert YTreeNode to byte array.
     *
     * @param yTreeNode YTreeNode.
     * @return Byte array.
     */
    public static byte[] serializeYTree(YTreeNode yTreeNode) {
        var baos = new ByteArrayOutputStream();
        YTreeBinarySerializer.serialize(yTreeNode, baos);
        return baos.toByteArray();
    }

    /**
     * Convert byte array to YTreeNode.
     *
     * @param binary Byte array.
     * @return YTreeNode.
     */
    public static YTreeNode yTreeFromBinary(byte[] binary) {
        return YTreeBinarySerializer.deserialize(
                new ByteArrayInputStream(binary)
        );
    }

    /**
     * Serializes an {@code @Entity}-annotated object (or any type supported by the default YSON
     * codec) into its binary YSON representation.
     *
     * @param value the value to serialize.
     * @param <T>   the value type.
     * @return the binary YSON bytes.
     */
    public static <T> byte[] serializeEntity(T value) {
        return DefaultYsonCodec.INSTANCE.encode(value);
    }

    /**
     * Deserializes a binary YSON byte array into an instance of {@code entityClass}, an
     * {@code @Entity}-annotated class (or any type supported by the default YSON codec).
     *
     * @param bytes       the binary YSON bytes.
     * @param entityClass the class of the object.
     * @param <T>         the value type.
     * @return the deserialized object.
     */
    @SuppressWarnings("unchecked")
    public static <T> T deserializeEntity(byte[] bytes, Class<T> entityClass) {
        return (T) DefaultYsonCodec.INSTANCE.decode(bytes, entityClass);
    }

    /**
     * Extracts an int value from a YTreeNode, returning a default if the node is null or entity.
     *
     * @param node         The YTreeNode to read from.
     * @param defaultValue The value to return when {@code node} is null or an entity node.
     * @return The int value of the node, or {@code defaultValue} if not present.
     */
    public static int toIntOrDefault(@Nullable YTreeNode node, int defaultValue) {
        if (node == null || node.isEntityNode()) {
            return defaultValue;
        }
        long value = node.longValue();
        return Math.toIntExact(value);
    }

    /**
     * Extracts a long value from a YTreeNode, returning a default if the node is null or entity.
     *
     * @param node         The YTreeNode to read from.
     * @param defaultValue The value to return when {@code node} is null or an entity node.
     * @return The long value of the node, or {@code defaultValue} if not present.
     */
    public static long toLongOrDefault(@Nullable YTreeNode node, long defaultValue) {
        if (node == null || node.isEntityNode()) {
            return defaultValue;
        }
        return node.longValue();
    }

    /**
     * Extracts a string value from a YTreeNode, returning a default if the node is null or entity.
     *
     * @param node         The YTreeNode to read from.
     * @param defaultValue The value to return when {@code node} is null or an entity node.
     * @return The string value of the node, or {@code defaultValue} if not present.
     */
    public static @Nullable String toStringOrDefault(@Nullable YTreeNode node, @Nullable String defaultValue) {
        if (node == null || node.isEntityNode()) {
            return defaultValue;
        }
        return node.stringValue();
    }
}
