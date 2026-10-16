package tech.ytsaurus.core.rows;

import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.junit.jupiter.api.Test;
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
import tech.ytsaurus.core.rows.simple.YTreeYPathSerializer;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeMapNode;
import tech.ytsaurus.ysontree.YTreeNode;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class YTreeSerializerFactoryTest {

    public enum Color {
        RED, GREEN, BLUE
    }

    // Holders for parameterized type extraction in tests.
    @SuppressWarnings("unused")
    private List<String> listOfString;
    @SuppressWarnings("unused")
    private Set<Long> setOfLong;
    @SuppressWarnings("unused")
    private Map<String, Long> mapStringLong;
    @SuppressWarnings("unused")
    private Map<Long, Long> mapLongLong;
    @SuppressWarnings("unused")
    private List<Object> listOfUnsupported;
    @SuppressWarnings("unused")
    private Map<String, Object> mapStringObject;
    @SuppressWarnings("unused")
    private List<String>[] arrayOfListOfString;

    private static Type fieldType(String name) {
        try {
            Field field = YTreeSerializerFactoryTest.class.getDeclaredField(name);
            return field.getGenericType();
        } catch (NoSuchFieldException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Test
    public void primitivesAndWrappers() {
        assertTrue(YTreeSerializerFactory.forClass(boolean.class) instanceof YTreeBooleanSerializer);
        assertTrue(YTreeSerializerFactory.forClass(Boolean.class) instanceof YTreeBooleanSerializer);
        assertTrue(YTreeSerializerFactory.forClass(byte.class) instanceof YTreeByteSerializer);
        assertTrue(YTreeSerializerFactory.forClass(Byte.class) instanceof YTreeByteSerializer);
        assertTrue(YTreeSerializerFactory.forClass(short.class) instanceof YTreeShortSerializer);
        assertTrue(YTreeSerializerFactory.forClass(Short.class) instanceof YTreeShortSerializer);
        assertTrue(YTreeSerializerFactory.forClass(int.class) instanceof YTreeIntegerSerializer);
        assertTrue(YTreeSerializerFactory.forClass(Integer.class) instanceof YTreeIntegerSerializer);
        assertTrue(YTreeSerializerFactory.forClass(long.class) instanceof YTreeLongSerializer);
        assertTrue(YTreeSerializerFactory.forClass(Long.class) instanceof YTreeLongSerializer);
        assertTrue(YTreeSerializerFactory.forClass(double.class) instanceof YTreeDoubleSerializer);
        assertTrue(YTreeSerializerFactory.forClass(Double.class) instanceof YTreeDoubleSerializer);
        assertTrue(YTreeSerializerFactory.forClass(float.class) instanceof YTreeFloatSerializer);
        assertTrue(YTreeSerializerFactory.forClass(Float.class) instanceof YTreeFloatSerializer);
    }

    @Test
    public void byteAndShortDeserializeToExactJavaTypes() {
        assertEquals(Byte.valueOf((byte) 42),
                YTreeSerializerFactory.forClass(byte.class).deserialize(YTree.integerNode(42)));
        assertEquals(Short.valueOf((short) 300),
                YTreeSerializerFactory.forClass(short.class).deserialize(YTree.integerNode(300)));
    }

    @Test
    public void byteAndShortRejectOutOfRangeValues() {
        assertThrows(IllegalArgumentException.class,
                () -> YTreeSerializerFactory.forClass(byte.class).deserialize(YTree.integerNode(128)));
        assertThrows(IllegalArgumentException.class,
                () -> YTreeSerializerFactory.forClass(short.class).deserialize(YTree.integerNode(32768)));
    }

    @Test
    public void simpleTypes() {
        assertTrue(YTreeSerializerFactory.forClass(String.class) instanceof YTreeStringSerializer);
        assertTrue(YTreeSerializerFactory.forClass(byte[].class) instanceof YTreeBytesSerializer);
        assertTrue(YTreeSerializerFactory.forClass(Instant.class) instanceof YTreeInstantSerializer);
        assertTrue(YTreeSerializerFactory.forClass(Duration.class) instanceof YTreeDurationSerializer);
        assertTrue(YTreeSerializerFactory.forClass(LocalDateTime.class) instanceof YTreeLocalDateTimeSerializer);
        assertTrue(YTreeSerializerFactory.forClass(OffsetDateTime.class) instanceof YTreeOffsetDateTimeSerializer);
        assertTrue(YTreeSerializerFactory.forClass(YPath.class) instanceof YTreeYPathSerializer);
    }

    @Test
    public void cachingReturnsSameInstance() {
        YTreeSerializer<String> a = YTreeSerializerFactory.forClass(String.class);
        YTreeSerializer<String> b = YTreeSerializerFactory.forClass(String.class);
        assertSame(a, b);
    }

    @Test
    public void enums() {
        assertTrue(YTreeSerializerFactory.forClass(Color.class) instanceof YTreeEnumSerializer);
    }

    @Test
    public void listOfStringSerializer() {
        Optional<YTreeSerializer<?>> serializer = YTreeSerializerFactory.forType(fieldType("listOfString"));
        assertTrue(serializer.isPresent());
        assertTrue(serializer.get() instanceof YTreeListSerializer);
        YTreeListSerializer<?> listSerializer = (YTreeListSerializer<?>) serializer.get();
        assertEquals(String.class, listSerializer.getElementType());
    }

    @Test
    public void setOfLongSerializer() {
        Optional<YTreeSerializer<?>> serializer = YTreeSerializerFactory.forType(fieldType("setOfLong"));
        assertTrue(serializer.isPresent());
        assertTrue(serializer.get() instanceof YTreeSetSerializer);
    }

    @Test
    public void mapStringLongSerializer() {
        Optional<YTreeSerializer<?>> serializer = YTreeSerializerFactory.forType(fieldType("mapStringLong"));
        assertTrue(serializer.isPresent());
        assertTrue(serializer.get() instanceof YTreeMapSerializer);
    }

    @Test
    public void mapWithNonStringKeyFails() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> YTreeSerializerFactory.forType(fieldType("mapLongLong")));
        assertTrue(ex.getMessage().contains("Only Map<String, T> is supported"));
    }

    @Test
    public void collectionWithUnsupportedElementReturnsEmpty() {
        // List<Object> -> Object is not supported -> the whole type should be empty.
        assertFalse(YTreeSerializerFactory.forType(fieldType("listOfUnsupported")).isPresent());
    }

    @Test
    public void mapWithUnsupportedValueReturnsEmpty() {
        assertFalse(YTreeSerializerFactory.forType(fieldType("mapStringObject")).isPresent());
    }

    @Test
    public void rawContainersReturnEmpty() {
        assertFalse(YTreeSerializerFactory.forType(List.class).isPresent());
        assertFalse(YTreeSerializerFactory.forType(Set.class).isPresent());
        assertFalse(YTreeSerializerFactory.forType(Map.class).isPresent());
    }

    @Test
    public void concreteCollectionsAreUnsupported() {
        assertFalse(YTreeSerializerFactory.forType(ArrayList.class).isPresent());
    }

    @Test
    public void nestedResolverCanExtendCollectionElementSupport() {
        Optional<YTreeSerializer<?>> serializer = YTreeSerializerFactory.forTypeWithNestedResolver(
                fieldType("listOfUnsupported"),
                nestedType -> nestedType.equals(Object.class)
                        ? Optional.of(YTreeSerializerFactory.forClass(String.class))
                        : YTreeSerializerFactory.forType(nestedType)
        );

        assertTrue(serializer.isPresent());
        assertTrue(serializer.get() instanceof YTreeListSerializer);
    }

    @Test
    public void genericArrayKeepsParameterizedComponentType() {
        Optional<YTreeSerializer<?>> serializer = YTreeSerializerFactory.forType(fieldType("arrayOfListOfString"));
        assertTrue(serializer.isPresent());
        assertTrue(serializer.get() instanceof YTreeArraySerializer);
        YTreeArraySerializer<?, ?> arraySerializer = (YTreeArraySerializer<?, ?>) serializer.get();
        assertTrue(arraySerializer.getComponent() instanceof YTreeListSerializer);
    }

    @Test
    public void yTreeNodeTypes() {
        assertTrue(YTreeSerializerFactory.forType(YTreeNode.class).isPresent());
        assertTrue(YTreeSerializerFactory.forClass(YTreeMapNode.class) instanceof YTreeMapNodeSerializer);
    }

    @Test
    public void unsupportedTypeForClassThrows() {
        assertThrows(IllegalArgumentException.class,
                () -> YTreeSerializerFactory.forClass(Object.class));
        assertThrows(IllegalArgumentException.class,
                () -> YTreeSerializerFactory.forClass(char.class));
        assertThrows(IllegalArgumentException.class,
                () -> YTreeSerializerFactory.forClass(void.class));
    }

    @Test
    public void unsupportedTypeForTypeReturnsEmpty() {
        assertFalse(YTreeSerializerFactory.forType(Object.class).isPresent());
        assertFalse(YTreeSerializerFactory.forType(java.math.BigDecimal.class).isPresent());
        assertFalse(YTreeSerializerFactory.forType(char.class).isPresent());
        assertFalse(YTreeSerializerFactory.forType(Character.class).isPresent());
        assertFalse(YTreeSerializerFactory.forType(void.class).isPresent());
        assertFalse(YTreeSerializerFactory.forType(Void.class).isPresent());
    }

    @Test
    public void primitiveWrapHelpers() {
        assertEquals(Integer.class, YTreeSerializerFactory.wrapPrimitive(int.class));
        assertEquals(Long.class, YTreeSerializerFactory.wrapPrimitive(long.class));
        assertEquals(String.class, YTreeSerializerFactory.wrapPrimitive(String.class));
    }
}
