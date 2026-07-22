package tech.ytsaurus.flow.utils;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.ConcurrentHashMap;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageLite;
import com.google.protobuf.Parser;
import tech.ytsaurus.TGuid;
import tech.ytsaurus.core.GUID;

/**
 * Helper class for interaction with protobuf messages.
 */
public class ProtoUtils {

    /**
     * Cache for generated {@link Parser}s, keyed by message class. The parser is resolved once via
     * reflection and reused for every subsequent decode.
     */
    private static final ConcurrentHashMap<Class<?>, Parser<?>> PARSER_CACHE = new ConcurrentHashMap<>();

    /**
     * Cache for {@code newBuilder()} methods, keyed by message class.
     */
    private static final ConcurrentHashMap<Class<?>, Method> NEW_BUILDER_CACHE = new ConcurrentHashMap<>();

    private ProtoUtils() {
    }

    public static GUID fromProto(TGuid protoGuid) {
        return new GUID(protoGuid.getFirst(), protoGuid.getSecond());
    }

    public static TGuid toProto(GUID guid) {
        return TGuid.newBuilder()
                .setFirst(guid.getFirst())
                .setSecond(guid.getSecond())
                .build();
    }

    /**
     * Resolves and caches the generated {@link Parser} for the given message class.
     * The parser is obtained from the message's static {@code getDefaultInstance()} once per class.
     *
     * @param messageClass The class of the protobuf message.
     * @param <T>          The type of the protobuf message.
     * @return The cached {@link Parser} for the message class.
     * @throws RuntimeException if the parser cannot be resolved via reflection.
     */
    @SuppressWarnings("unchecked")
    private static <T extends MessageLite> Parser<T> parserFor(Class<T> messageClass) {
        return (Parser<T>) PARSER_CACHE.computeIfAbsent(messageClass, clazz -> {
            try {
                var defaultInstance = (MessageLite) clazz.getMethod("getDefaultInstance").invoke(null);
                return defaultInstance.getParserForType();
            } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        });
    }

    /**
     * Parses the given byte array into a message of the specified class.
     * Uses a cached {@link Parser} so no reflection is performed per decode.
     *
     * @param bytes        The byte array to parse.
     * @param messageClass The class of the message to parse into.
     * @param <T>          The type of the message.
     * @return The parsed message.
     * @throws RuntimeException If the parsing fails.
     */
    public static <T extends MessageLite> T parseBytes(byte[] bytes, Class<T> messageClass) {
        try {
            return parserFor(messageClass).parseFrom(bytes);
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Parses a {@link ByteString} into an instance of the specified protobuf message class.
     * Uses a cached {@link Parser} so no reflection is performed per decode.
     *
     * @param bytes        The {@link ByteString} to parse.
     * @param messageClass The class of the protobuf message to parse into.
     * @param <T>          The type of the protobuf message.
     * @return The parsed message.
     * @throws RuntimeException if the parsing fails.
     */
    public static <T extends MessageLite> T parseBytes(ByteString bytes, Class<T> messageClass) {
        try {
            return parserFor(messageClass).parseFrom(bytes);
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Creates a new builder instance for the specified {@link MessageLite} class.
     * Uses a cached method invocation to call the {@code newBuilder} method of the given class.
     *
     * @param messageClass The class for which to create a builder.
     * @param <T>          The type of the builder, which extends {@link MessageLite.Builder}.
     * @return A new builder instance of the specified message class.
     * @throws RuntimeException if the builder cannot be created due to reflection issues or if the method is not found.
     */
    @SuppressWarnings("unchecked")
    public static <T> T newBuilder(Class<? extends MessageLite> messageClass) {
        try {
            var method = NEW_BUILDER_CACHE.computeIfAbsent(messageClass, clazz -> {
                try {
                    return clazz.getMethod("newBuilder");
                } catch (NoSuchMethodException e) {
                    throw new RuntimeException(e);
                }
            });
            return (T) method.invoke(null);
        } catch (InvocationTargetException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }
}
