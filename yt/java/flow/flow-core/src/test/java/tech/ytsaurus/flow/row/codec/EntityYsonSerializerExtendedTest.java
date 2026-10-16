package tech.ytsaurus.flow.row.codec;

import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.persistence.Entity;

import org.junit.jupiter.api.Test;
import tech.ytsaurus.flow.test.TTestMessage;
import tech.ytsaurus.flow.utils.YsonUtils;
import tech.ytsaurus.yson.YsonParser;
import tech.ytsaurus.ysontree.YTreeBuilder;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Exercises {@link EntityYsonSerializer} on extended cases: protobuf fields, arrays, collections,
 * nested generics, nested entities, multibyte strings, and rejection of non-entity and cyclic
 * entity classes.
 */
class EntityYsonSerializerExtendedTest {

    @Entity
    static class ProtoHolder {
        String id;
        TTestMessage message;
    }

    @Entity
    static class ArraysHolder {
        int[] ints;
        String[] strings;
    }

    @Entity
    static class CollectionsHolder {
        Set<String> set;
        Map<String, List<Integer>> nested;
    }

    @Entity
    static class Utf8Holder {
        String text;
    }

    @Entity
    static class Inner {
        String label;
        Integer num;
    }

    @Entity
    static class Outer {
        String id;
        Inner inner;
    }

    @Entity
    static class SelfRef {
        String v;
        SelfRef next;
    }

    @Entity
    static class CycleA {
        String v;
        CycleB b;
    }

    @Entity
    static class CycleB {
        String v;
        CycleA a;
    }

    @Entity
    static class CycleListHolder {
        String v;
        List<CycleListHolder> children;
    }

    static class NotAnEntity {
        String name;
    }

    private static TTestMessage message() {
        return TTestMessage.newBuilder()
                .setId("m1")
                .setTime(123L)
                .setCount(7L)
                .setValue(2.5)
                .setIsValue(true)
                .build();
    }

    @Test
    void testProtobufFieldRoundTrip() {
        var serializer = new EntityYsonSerializer<>(ProtoHolder.class);
        var holder = new ProtoHolder();
        holder.id = "h";
        holder.message = message();

        var decoded = serializer.deserialize(serializer.serialize(holder));
        assertEquals("h", decoded.id);
        assertEquals(holder.message, decoded.message);
    }

    @Test
    void testProtobufFieldIsYsonStringOfWireBytes() {
        var serializer = new EntityYsonSerializer<>(ProtoHolder.class);
        var holder = new ProtoHolder();
        holder.id = "h";
        holder.message = message();

        var node = YsonUtils.yTreeFromBinary(serializer.serialize(holder));
        var messageNode = node.mapNode().asMap().get("message");
        assertArrayEquals(holder.message.toByteArray(), messageNode.bytesValue());
    }

    @Test
    void testNullProtobufRoundTrip() {
        var serializer = new EntityYsonSerializer<>(ProtoHolder.class);
        var holder = new ProtoHolder();
        holder.id = "h";
        holder.message = null;

        var node = YsonUtils.yTreeFromBinary(serializer.serialize(holder));
        assertTrue(node.mapNode().asMap().get("message").isEntityNode());

        var decoded = serializer.deserialize(serializer.serialize(holder));
        assertEquals("h", decoded.id);
        assertNull(decoded.message);
    }

    @Test
    void testArraysRoundTrip() {
        var serializer = new EntityYsonSerializer<>(ArraysHolder.class);
        var holder = new ArraysHolder();
        holder.ints = new int[]{1, 2, 3};
        holder.strings = new String[]{"a", "b"};

        var decoded = serializer.deserialize(serializer.serialize(holder));
        assertArrayEquals(holder.ints, decoded.ints);
        assertArrayEquals(holder.strings, decoded.strings);
    }

    @Test
    void testCollectionsRoundTrip() {
        var serializer = new EntityYsonSerializer<>(CollectionsHolder.class);
        var holder = new CollectionsHolder();
        holder.set = Set.of("x", "y");
        holder.nested = Map.of("a", List.of(1, 2), "b", List.of(3));

        var decoded = serializer.deserialize(serializer.serialize(holder));
        assertEquals(holder.set, decoded.set);
        assertEquals(holder.nested, decoded.nested);
    }

    @Test
    void testEmptyCollectionsRoundTrip() {
        var serializer = new EntityYsonSerializer<>(CollectionsHolder.class);
        var holder = new CollectionsHolder();
        holder.set = Set.of();
        holder.nested = Map.of();

        var decoded = serializer.deserialize(serializer.serialize(holder));
        assertEquals(Set.of(), decoded.set);
        assertEquals(Map.of(), decoded.nested);
    }

    @Test
    void testMultibyteUtf8RoundTrip() {
        var serializer = new EntityYsonSerializer<>(Utf8Holder.class);
        var holder = new Utf8Holder();
        holder.text = "Привет, 世界 🚀";

        var decoded = serializer.deserialize(serializer.serialize(holder));
        assertEquals(holder.text, decoded.text);

        // text-YSON path too
        byte[] textBytes = serializer.serialize(holder, false);
        var builder = new YTreeBuilder();
        new YsonParser(textBytes).parseNode(builder);
        assertEquals(holder.text, serializer.deserialize(builder.build()).text);
    }

    @Test
    void testNestedEntityRoundTrip() {
        var serializer = new EntityYsonSerializer<>(Outer.class);
        var outer = new Outer();
        outer.id = "o";
        outer.inner = new Inner();
        outer.inner.label = "L";
        outer.inner.num = 5;

        var node = YsonUtils.yTreeFromBinary(serializer.serialize(outer));
        var innerNode = node.mapNode().asMap().get("inner");
        assertTrue(innerNode.isMapNode());
        assertEquals("L", innerNode.mapNode().asMap().get("label").stringValue());

        var decoded = serializer.deserialize(serializer.serialize(outer));
        assertEquals("o", decoded.id);
        assertEquals("L", decoded.inner.label);
        assertEquals(5, decoded.inner.num);
    }

    @Test
    void testNestedEntityNullRoundTrip() {
        var serializer = new EntityYsonSerializer<>(Outer.class);
        var outer = new Outer();
        outer.id = "o";
        outer.inner = null;

        var decoded = serializer.deserialize(serializer.serialize(outer));
        assertEquals("o", decoded.id);
        assertNull(decoded.inner);
    }

    @Test
    void testFactoryResolvesEntitySerializer() {
        var resolved = CodecRegistry.getInstance().getYsonCodec().getOrCreateSerializer(Inner.class);
        assertTrue(resolved instanceof EntityYsonSerializer);
    }

    @Test
    void testNotAnEntityThrows() {
        assertThrows(IllegalArgumentException.class,
                () -> new EntityYsonSerializer<>(NotAnEntity.class));
    }

    @Test
    void testSelfReferentialEntityThrows() {
        var ex = assertThrows(IllegalArgumentException.class,
                () -> new EntityYsonSerializer<>(SelfRef.class));
        assertTrue(ex.getMessage().contains("cyclic field reference"), ex.getMessage());
    }

    @Test
    void testMutuallyRecursiveEntitiesThrow() {
        assertThrows(IllegalArgumentException.class,
                () -> new EntityYsonSerializer<>(CycleA.class));
        assertThrows(IllegalArgumentException.class,
                () -> new EntityYsonSerializer<>(CycleB.class));
    }

    @Test
    void testCycleThroughCollectionThrows() {
        assertThrows(IllegalArgumentException.class,
                () -> new EntityYsonSerializer<>(CycleListHolder.class));
    }

    @Test
    void testCycleDetectedViaFactory() {
        assertThrows(IllegalArgumentException.class,
                () -> CodecRegistry.getInstance().getYsonCodec().getOrCreateSerializer(SelfRef.class));
    }

    @Test
    void testDeserializeNonMapNodeThrows() {
        var serializer = new EntityYsonSerializer<>(Utf8Holder.class);
        var node = new YTreeBuilder().value("not-a-map").build();
        var ex = assertThrows(IllegalArgumentException.class, () -> serializer.deserialize(node));
        assertTrue(ex.getMessage().contains("Expected a YSON map"), ex.getMessage());
    }
}
