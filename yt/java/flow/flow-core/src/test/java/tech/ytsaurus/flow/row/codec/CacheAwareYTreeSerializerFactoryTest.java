package tech.ytsaurus.flow.row.codec;

import java.lang.reflect.GenericArrayType;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import tech.ytsaurus.core.rows.YTreeListSerializer;
import tech.ytsaurus.core.rows.YTreeMapSerializer;
import tech.ytsaurus.core.rows.YTreeSerializer;
import tech.ytsaurus.core.rows.simple.YTreeIntegerSerializer;
import tech.ytsaurus.core.rows.simple.YTreeLongSerializer;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;


class CacheAwareYTreeSerializerFactoryTest {

    @SuppressWarnings("unused")
    private Map<String, Long> longMap;

    @SuppressWarnings("unused")
    private Map<String, Integer> integerMap;

    @SuppressWarnings("unused")
    private Map<String, List<Long>> nestedMap;

    @SuppressWarnings("unused")
    private List<String>[] genericArray;

    @Test
    void resolvesDistinctSerializersForDifferentMapValueTypes() throws NoSuchFieldException {
        Type longMapType = getClass().getDeclaredField("longMap").getGenericType();
        Type integerMapType = getClass().getDeclaredField("integerMap").getGenericType();

        YTreeSerializer<?> longMapSerializer = CacheAwareYTreeSerializerFactory.INSTANCE
                .forType(longMapType);
        YTreeSerializer<?> integerMapSerializer = CacheAwareYTreeSerializerFactory.INSTANCE
                .forType(integerMapType);

        var typedLongMapSerializer = assertInstanceOf(YTreeMapSerializer.class, longMapSerializer);
        var typedIntegerMapSerializer = assertInstanceOf(YTreeMapSerializer.class, integerMapSerializer);

        assertNotEquals(typedLongMapSerializer, typedIntegerMapSerializer);
        assertInstanceOf(YTreeLongSerializer.class, typedLongMapSerializer.getComponent());
        assertInstanceOf(YTreeIntegerSerializer.class, typedIntegerMapSerializer.getComponent());
    }

    /**
     * Regression test: resolving a parameterized type requires resolving its element types,
     * which re-enters {@link CacheAwareYTreeSerializerFactory#forType} on a cold cache. The
     * implementation must not perform nested updates inside
     * {@link java.util.concurrent.ConcurrentHashMap#computeIfAbsent}.
     */
    @Test
    void resolvesDeeplyNestedParameterizedType() throws NoSuchFieldException {
        Type nestedMapType = getClass().getDeclaredField("nestedMap").getGenericType();

        YTreeSerializer<?> serializer = CacheAwareYTreeSerializerFactory.INSTANCE
                .forType(nestedMapType);

        var mapSerializer = assertInstanceOf(YTreeMapSerializer.class, serializer);
        var listSerializer = assertInstanceOf(YTreeListSerializer.class, mapSerializer.getComponent());
        assertInstanceOf(YTreeLongSerializer.class, listSerializer.getComponent());
    }

    @Test
    void cachesSerializerInstanceAcrossCalls() {
        YTreeSerializer<?> first = CacheAwareYTreeSerializerFactory.INSTANCE
                .forType(Long.class);
        YTreeSerializer<?> second = CacheAwareYTreeSerializerFactory.INSTANCE
                .forType(Long.class);

        assertSame(first, second);
    }

    /**
     * Regression test: the cache must key {@link GenericArrayType} as well as {@link Class}
     * and {@link java.lang.reflect.ParameterizedType}, relying on the JDK contract that all
     * {@link Type} kinds implement structural {@code equals}/{@code hashCode}.
     */
    @Test
    void cachesGenericArrayType() throws NoSuchFieldException {
        Type first = getClass().getDeclaredField("genericArray").getGenericType();
        Type second = getClass().getDeclaredField("genericArray").getGenericType();
        assertInstanceOf(GenericArrayType.class, first);

        YTreeSerializer<?> firstSerializer = CacheAwareYTreeSerializerFactory.INSTANCE
                .forType(first);
        YTreeSerializer<?> secondSerializer = CacheAwareYTreeSerializerFactory.INSTANCE
                .forType(second);

        assertSame(firstSerializer, secondSerializer);
    }

    @Test
    void throwsOnUnsupportedType() {
        assertThrows(
                IllegalArgumentException.class,
                () -> CacheAwareYTreeSerializerFactory.INSTANCE.forType(Unsupported.class)
        );
    }

    /**
     * Regression test: subclasses might override {@link CacheAwareYTreeSerializerFactory#compute}
     * to resolve nested element types by calling {@link CacheAwareYTreeSerializerFactory#forType} recursively.
     * That re-enters the cache during an in-progress update, which a
     * {@link java.util.concurrent.ConcurrentHashMap#computeIfAbsent}
     * implementation forbids and detects with {@code IllegalStateException("Recursive update")}
     * when the inner key lands in the same bin as the outer key.
     */
    @Test
    void allowsRecursiveForTypeFromCompute() {
        Type outer = new FixedHashType("outer");
        Type inner = new FixedHashType("inner");

        var factory = new CacheAwareYTreeSerializerFactory() {
            @Override
            protected YTreeSerializer<?> compute(Type type) {
                if (type == outer) {
                    YTreeSerializer<?> nested = forType(inner);
                    assertNotNull(nested);
                    return new YTreeLongSerializer();
                }
                return new YTreeIntegerSerializer();
            }
        };

        YTreeSerializer<?> serializer = assertDoesNotThrow(() -> factory.forType(outer));
        assertInstanceOf(YTreeLongSerializer.class, serializer);
        assertInstanceOf(YTreeIntegerSerializer.class, factory.forType(inner));
    }

    private static final class Unsupported {
    }

    /**
     * {@link Type} key with a constant {@code hashCode} so that distinct instances are placed
     * in the same {@link java.util.concurrent.ConcurrentHashMap} bin, forcing the JDK's
     * recursive-update detection to fire on a re-entrant {@code computeIfAbsent} call.
     */
    private static final class FixedHashType implements Type {
        private final String name;

        FixedHashType(String name) {
            this.name = name;
        }

        @Override
        public String getTypeName() {
            return name;
        }

        @Override
        public int hashCode() {
            return 0;
        }

        @Override
        public boolean equals(Object o) {
            return o == this;
        }

        @Override
        public String toString() {
            return name;
        }
    }
}
