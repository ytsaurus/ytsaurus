package tech.ytsaurus.core.rows;

import java.lang.reflect.GenericArrayType;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import tech.ytsaurus.core.StringValueEnum;
import tech.ytsaurus.core.StringValueEnumResolver;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.core.rows.simple.YTreeBooleanSerializer;
import tech.ytsaurus.core.rows.simple.YTreeByteSerializer;
import tech.ytsaurus.core.rows.simple.YTreeDoubleSerializer;
import tech.ytsaurus.core.rows.simple.YTreeDurationSerializer;
import tech.ytsaurus.core.rows.simple.YTreeEnumSerializer;
import tech.ytsaurus.core.rows.simple.YTreeFloatSerializer;
import tech.ytsaurus.core.rows.simple.YTreeInstantSerializer;
import tech.ytsaurus.core.rows.simple.YTreeIntegerSerializer;
import tech.ytsaurus.core.rows.simple.YTreeLocalDateTimeSerializer;
import tech.ytsaurus.core.rows.simple.YTreeLongSerializer;
import tech.ytsaurus.core.rows.simple.YTreeOffsetDateTimeSerializer;
import tech.ytsaurus.core.rows.simple.YTreeShortSerializer;
import tech.ytsaurus.core.rows.simple.YTreeStringSerializer;
import tech.ytsaurus.core.rows.simple.YTreeStringValueEnumSerializer;
import tech.ytsaurus.core.rows.simple.YTreeYPathSerializer;
import tech.ytsaurus.core.utils.ClassUtils;
import tech.ytsaurus.typeinfo.TiType;
import tech.ytsaurus.yson.YsonConsumer;
import tech.ytsaurus.ysontree.YTreeMapNode;
import tech.ytsaurus.ysontree.YTreeNode;
import tech.ytsaurus.ysontree.YTreeNodeUtils;

/**
 * Resolves a {@link YTreeSerializer} for a given Java {@link Type}.
 * <p>
 * Supported types are primitive types and their wrappers (except {@code char} and {@code void}), {@link String},
 * {@code byte[]}, java.time types, {@link YPath}, enums, {@link StringValueEnum}, {@link YTreeNode},
 * {@link YTreeMapNode}, arrays, and exact JDK collection interfaces ({@link List}, {@link Set},
 * {@code Map<String, T>}). Raw collections and concrete collection classes are not supported.
 */
public final class YTreeSerializerFactory {

    private static final YTreeSerializer<Boolean> BOOLEAN_SERIALIZER = new YTreeBooleanSerializer();
    private static final YTreeSerializer<Byte> BYTE_SERIALIZER = new YTreeByteSerializer();
    private static final YTreeSerializer<Short> SHORT_SERIALIZER = new YTreeShortSerializer();
    private static final YTreeSerializer<Integer> INTEGER_SERIALIZER = new YTreeIntegerSerializer();
    private static final YTreeSerializer<Long> LONG_SERIALIZER = new YTreeLongSerializer();
    private static final YTreeSerializer<Double> DOUBLE_SERIALIZER = new YTreeDoubleSerializer();
    private static final YTreeSerializer<Float> FLOAT_SERIALIZER = new YTreeFloatSerializer();
    private static final YTreeSerializer<String> STRING_SERIALIZER = new YTreeStringSerializer();
    private static final YTreeSerializer<byte[]> BYTES_SERIALIZER = new YTreeBytesSerializer();
    private static final YTreeSerializer<Instant> JAVA_INSTANT_SERIALIZER = new YTreeInstantSerializer();
    private static final YTreeSerializer<Duration> JAVA_DURATION_SERIALIZER = new YTreeDurationSerializer();
    private static final YTreeSerializer<LocalDateTime> LOCAL_DATE_TIME_SERIALIZER = new YTreeLocalDateTimeSerializer();
    private static final YTreeSerializer<OffsetDateTime> OFFSET_DATE_TIME_SERIALIZER =
            new YTreeOffsetDateTimeSerializer();
    private static final YTreeSerializer<YPath> Y_PATH_SERIALIZER = new YTreeYPathSerializer();
    private static final YTreeSerializer<YTreeMapNode> Y_TREE_MAP_NODE_SERIALIZER = new YTreeMapNodeSerializer();

    private static final YTreeSerializer<YTreeNode> Y_TREE_SERIALIZER = new YTreeSerializer<>() {
        @Override
        public void serialize(YTreeNode obj, YsonConsumer consumer) {
            YTreeNodeUtils.walk(obj, consumer, true);
        }

        @Override
        public YTreeNode deserialize(YTreeNode node) {
            return node;
        }

        @Override
        public TiType getColumnValueType() {
            return TiType.optional(TiType.yson());
        }
    };

    private static final Map<Class<?>, YTreeSerializer<?>> SIMPLE_SERIALIZERS = createSimpleSerializers();
    private static final Map<Class<?>, Class<?>> PRIMITIVE_TO_WRAPPER = createPrimitiveToWrapperMap();

    private YTreeSerializerFactory() {
    }

    /**
     * Resolves a serializer for {@code clazz}.
     *
     * @throws IllegalArgumentException if {@code clazz} is not supported
     */
    @SuppressWarnings("unchecked")
    public static <T> YTreeSerializer<T> forClass(Class<T> clazz) {
        return (YTreeSerializer<T>) forType(clazz)
                .orElseThrow(() -> new IllegalArgumentException("Can't construct serializer for " + clazz));
    }

    /**
     * Tries to resolve a serializer for {@code type}. Returns empty if the type is not supported.
     */
    public static Optional<YTreeSerializer<?>> forType(Type type) {
        return forTypeWithNestedResolver(type, YTreeSerializerFactory::forType);
    }

    /**
     * Tries to resolve built-in scalar, array, and standard JDK collection serializers.
     * The {@code nestedResolver} is used to resolve array and collection element types.
     * <p>
     * This method is intended for serializers that extend the set of supported nested types while reusing the standard
     * container logic of this factory.
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    public static Optional<YTreeSerializer<?>> forTypeWithNestedResolver(
            Type type,
            Function<Type, Optional<YTreeSerializer<?>>> nestedResolver
    ) {
        Class clazz = ClassUtils.erasure(type);
        YTreeSerializer<?> simpleSerializer = SIMPLE_SERIALIZERS.get(clazz);
        if (simpleSerializer != null) {
            return Optional.of(simpleSerializer);
        }
        if (StringValueEnum.class.isAssignableFrom(clazz)) {
            return Optional.of(new YTreeStringValueEnumSerializer(StringValueEnumResolver.of(clazz)));
        }
        if (clazz.isEnum()) {
            return Optional.of(new YTreeEnumSerializer(clazz));
        }
        if (clazz.isArray()) {
            Type componentType = getComponentType(type);
            return nestedResolver.apply(componentType)
                    .map(serializer -> new YTreeArraySerializer<>(componentType, serializer));
        }
        if (clazz.equals(List.class)) {
            Optional<List<Type>> actualArguments = getActualTypeArguments(type);
            if (actualArguments.isEmpty()) {
                return Optional.empty();
            }
            Type elementType = actualArguments.get().get(0);
            return nestedResolver.apply(elementType)
                    .map(serializer -> new YTreeListSerializer<>(elementType, serializer));
        }
        if (clazz.equals(Set.class)) {
            Optional<List<Type>> actualArguments = getActualTypeArguments(type);
            if (actualArguments.isEmpty()) {
                return Optional.empty();
            }
            return nestedResolver.apply(actualArguments.get().get(0)).map(YTreeSetSerializer::new);
        }
        if (clazz.equals(Map.class)) {
            Optional<List<Type>> maybeActualArguments = getActualTypeArguments(type);
            if (maybeActualArguments.isEmpty()) {
                return Optional.empty();
            }
            List<Type> actualArguments = maybeActualArguments.get();
            if (actualArguments.size() != 2) {
                throw new IllegalArgumentException("Map type must have exactly two type arguments: " + type);
            }
            if (!actualArguments.get(0).equals(String.class)) {
                throw new IllegalArgumentException(
                        "Only Map<String, T> is supported, got key type: " + actualArguments.get(0));
            }
            return nestedResolver.apply(actualArguments.get(1)).map(YTreeMapSerializer::new);
        }
        return Optional.empty();
    }

    /**
     * Returns the wrapper class for a primitive type, or {@code clazz} unchanged if it is not primitive.
     */
    public static Class<?> wrapPrimitive(Class<?> clazz) {
        return PRIMITIVE_TO_WRAPPER.getOrDefault(clazz, clazz);
    }

    private static Type getComponentType(Type type) {
        if (type instanceof Class<?>) {
            Type componentType = ((Class<?>) type).getComponentType();
            if (componentType == null) {
                throw new IllegalArgumentException("Type is not an array: " + type);
            }
            return componentType;
        }
        if (type instanceof GenericArrayType) {
            return ((GenericArrayType) type).getGenericComponentType();
        }
        throw new IllegalArgumentException("Type is not an array: " + type);
    }

    private static Optional<List<Type>> getActualTypeArguments(Type type) {
        if (!(type instanceof ParameterizedType)) {
            return Optional.empty();
        }
        return Optional.of(ClassUtils.getActualTypeArguments(type));
    }

    private static Map<Class<?>, YTreeSerializer<?>> createSimpleSerializers() {
        Map<Class<?>, YTreeSerializer<?>> serializers = new HashMap<>();
        serializers.put(boolean.class, BOOLEAN_SERIALIZER);
        serializers.put(Boolean.class, BOOLEAN_SERIALIZER);
        serializers.put(byte.class, BYTE_SERIALIZER);
        serializers.put(Byte.class, BYTE_SERIALIZER);
        serializers.put(short.class, SHORT_SERIALIZER);
        serializers.put(Short.class, SHORT_SERIALIZER);
        serializers.put(int.class, INTEGER_SERIALIZER);
        serializers.put(Integer.class, INTEGER_SERIALIZER);
        serializers.put(long.class, LONG_SERIALIZER);
        serializers.put(Long.class, LONG_SERIALIZER);
        serializers.put(double.class, DOUBLE_SERIALIZER);
        serializers.put(Double.class, DOUBLE_SERIALIZER);
        serializers.put(float.class, FLOAT_SERIALIZER);
        serializers.put(Float.class, FLOAT_SERIALIZER);
        serializers.put(String.class, STRING_SERIALIZER);
        serializers.put(byte[].class, BYTES_SERIALIZER);
        serializers.put(Instant.class, JAVA_INSTANT_SERIALIZER);
        serializers.put(Duration.class, JAVA_DURATION_SERIALIZER);
        serializers.put(LocalDateTime.class, LOCAL_DATE_TIME_SERIALIZER);
        serializers.put(OffsetDateTime.class, OFFSET_DATE_TIME_SERIALIZER);
        serializers.put(YPath.class, Y_PATH_SERIALIZER);
        serializers.put(YTreeNode.class, Y_TREE_SERIALIZER);
        serializers.put(YTreeMapNode.class, Y_TREE_MAP_NODE_SERIALIZER);
        return Collections.unmodifiableMap(serializers);
    }

    private static Map<Class<?>, Class<?>> createPrimitiveToWrapperMap() {
        Map<Class<?>, Class<?>> primitives = new HashMap<>();
        primitives.put(void.class, Void.class);
        primitives.put(boolean.class, Boolean.class);
        primitives.put(byte.class, Byte.class);
        primitives.put(char.class, Character.class);
        primitives.put(short.class, Short.class);
        primitives.put(int.class, Integer.class);
        primitives.put(long.class, Long.class);
        primitives.put(float.class, Float.class);
        primitives.put(double.class, Double.class);
        return Collections.unmodifiableMap(primitives);
    }
}
