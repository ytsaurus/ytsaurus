package tech.ytsaurus.flow.row.codec;

import java.lang.reflect.Type;
import java.util.concurrent.ConcurrentHashMap;

import tech.ytsaurus.core.rows.YTreeSerializer;
import tech.ytsaurus.core.rows.YTreeSerializerFactory;
import tech.ytsaurus.core.utils.ClassUtils;
import tech.ytsaurus.flow.utils.AnnotationUtils;

/**
 * Cache of {@link YTreeSerializer} instances keyed by Java {@link Type}, so a serializer
 * for a given type is resolved at most once.
 */
class CacheAwareYTreeSerializerFactory {

    /**
     * Shared instance backed by {@link YTreeSerializerFactory#forType(Type)}.
     */
    static final CacheAwareYTreeSerializerFactory INSTANCE = new CacheAwareYTreeSerializerFactory();

    private final ConcurrentHashMap<Type, YTreeSerializer<?>> cache = new ConcurrentHashMap<>();

    protected CacheAwareYTreeSerializerFactory() {
    }

    /**
     * Resolves a serializer for {@code type}, returning a cached instance when available.
     *
     * <p>Computation happens outside any {@link ConcurrentHashMap} update so that nested
     * resolutions for parameterized element types can safely re-enter this method without
     * tripping {@link ConcurrentHashMap#computeIfAbsent}'s "no recursive updates" contract.
     *
     * @param type the Java type to resolve a serializer for
     * @return the resolved serializer
     */
    final YTreeSerializer<?> forType(Type type) {
        YTreeSerializer<?> cached = cache.get(type);
        if (cached != null) {
            return cached;
        }
        YTreeSerializer<?> computed = compute(type);
        YTreeSerializer<?> previous = cache.putIfAbsent(type, computed);
        return previous != null ? previous : computed;
    }

    /**
     * Computes a serializer for {@code type} on a cache miss.
     *
     * <p>Types known to {@link YTreeSerializerFactory} (scalars, collections, arrays, enums,
     * {@code YTreeNode}, ...) are resolved there; as a fallback, an {@code @Entity}-annotated
     * class is resolved to a serializer that (de)serializes it as a YSON map.
     *
     * @param type the Java type to resolve a serializer for
     * @return the resolved serializer
     * @throws IllegalArgumentException if {@code type} is unsupported
     */
    protected YTreeSerializer<?> compute(Type type) {
        var factorySerializer = YTreeSerializerFactory.forType(type);
        if (factorySerializer.isPresent()) {
            return factorySerializer.get();
        }
        return entitySerializerOrThrow(type);
    }

    /**
     * Resolves an {@code @Entity}-annotated class to a serializer that (de)serializes it as a YSON
     * map. Serves as the final fallback once the generic type resolution has been exhausted.
     *
     * @param type the type to resolve a serializer for
     * @return a serializer for the {@code @Entity}-annotated {@code type}
     * @throws IllegalArgumentException if {@code type} is not an {@code @Entity}-annotated class
     */
    protected final YTreeSerializer<?> entitySerializerOrThrow(Type type) {
        Class<?> clazz = ClassUtils.erasure(type);
        if (ClassUtils.anyOfAnnotationsPresent(clazz, AnnotationUtils.entityAnnotations())) {
            return new EntityYsonSerializer<>(clazz);
        }
        throw new IllegalArgumentException("Cannot construct serializer for " + type);
    }
}
