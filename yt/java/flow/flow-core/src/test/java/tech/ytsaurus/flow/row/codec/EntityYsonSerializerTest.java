package tech.ytsaurus.flow.row.codec;

import java.util.List;
import java.util.Map;

import javax.persistence.Entity;
import javax.persistence.Transient;

import org.junit.jupiter.api.Test;
import tech.ytsaurus.flow.utils.YsonUtils;
import tech.ytsaurus.yson.YsonParser;
import tech.ytsaurus.ysontree.YTreeBuilder;
import tech.ytsaurus.ysontree.YTreeNode;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies that {@link EntityYsonSerializer} serializes entities to YSON maps and reconstructs
 * them, covering scalar, collection, enum and {@code null} fields across the binary and text forms.
 */
class EntityYsonSerializerTest {

    enum Color {
        RED, GREEN, BLUE
    }

    @Entity
    static class Pojo {
        String name;
        Integer age;
        Boolean active;
        Double score;
        Long count;
        byte[] data;
        Color color;
        List<String> tags;
        Map<String, Integer> counts;
        String nickname;
    }

    @Entity
    static class WithTransient {
        String name;
        @Transient
        String temporaryData;
    }

    @Entity
    static class WithPrimitives {
        long count;
        int size;
        boolean active;
    }

    private static Pojo sample() {
        var p = new Pojo();
        p.name = "flow";
        p.age = 30;
        p.active = true;
        p.score = 3.14;
        p.count = 42L;
        p.data = new byte[]{1, 2, 3};
        p.color = Color.GREEN;
        p.tags = List.of("a", "b");
        p.counts = Map.of("x", 1, "y", 2);
        p.nickname = "fl";
        return p;
    }

    private static YTreeNode toNode(EntityYsonSerializer<Pojo> serializer, Pojo obj) {
        var builder = new YTreeBuilder();
        serializer.serialize(obj, builder);
        return builder.build();
    }

    private static void assertPojoEquals(Pojo expected, Pojo actual) {
        assertEquals(expected.name, actual.name);
        assertEquals(expected.age, actual.age);
        assertEquals(expected.active, actual.active);
        assertEquals(expected.score, actual.score);
        assertEquals(expected.count, actual.count);
        assertArrayEquals(expected.data, actual.data);
        assertEquals(expected.color, actual.color);
        assertEquals(expected.tags, actual.tags);
        assertEquals(expected.counts, actual.counts);
        assertEquals(expected.nickname, actual.nickname);
    }

    // ---- Stage 3: serialization ----

    @Test
    void testSerializeToYTreeNode() {
        var serializer = new EntityYsonSerializer<>(Pojo.class);
        var node = toNode(serializer, sample());

        assertTrue(node.isMapNode());
        var map = node.mapNode().asMap();

        var columnNames = serializer.getEntityTableSchema().getColumns().stream()
                .map(c -> c.getName())
                .toList();
        assertTrue(map.keySet().containsAll(columnNames));

        assertEquals("flow", map.get("name").stringValue());
        assertEquals(30, map.get("age").intValue());
        assertTrue(map.get("active").boolValue());
        assertEquals(3.14, map.get("score").doubleValue());
        assertEquals(42L, map.get("count").longValue());
        assertArrayEquals(new byte[]{1, 2, 3}, map.get("data").bytesValue());
        assertEquals("GREEN", map.get("color").stringValue());
        assertEquals(List.of("a", "b"),
                map.get("tags").asList().stream().map(YTreeNode::stringValue).toList());
        assertEquals(2, map.get("counts").mapNode().asMap().size());
        assertEquals("fl", map.get("nickname").stringValue());
    }

    @Test
    void testNullFieldSerializesAsEntity() {
        var serializer = new EntityYsonSerializer<>(Pojo.class);
        var obj = sample();
        obj.nickname = null;

        var map = toNode(serializer, obj).mapNode().asMap();
        assertTrue(map.get("nickname").isEntityNode());
    }

    @Test
    void testBinaryProducesEquivalentTree() {
        var serializer = new EntityYsonSerializer<>(Pojo.class);
        var obj = sample();

        var builderNode = toNode(serializer, obj);
        var fromBinary = YsonUtils.yTreeFromBinary(serializer.serialize(obj));
        assertEquals(builderNode, fromBinary);
    }

    @Test
    void testTransientFieldOmitted() {
        var serializer = new EntityYsonSerializer<>(WithTransient.class);
        var obj = new WithTransient();
        obj.name = "n";
        obj.temporaryData = "tmp";

        var builder = new YTreeBuilder();
        serializer.serialize(obj, builder);
        var map = builder.build().mapNode().asMap();

        assertTrue(map.containsKey("name"));
        assertFalse(map.containsKey("temporaryData"));
    }

    // ---- Stage 5: deserialization round-trips ----

    @Test
    void testBinaryRoundTrip() {
        var serializer = new EntityYsonSerializer<>(Pojo.class);
        var obj = sample();
        var decoded = serializer.deserialize(serializer.serialize(obj));
        assertPojoEquals(obj, decoded);
    }

    @Test
    void testBinaryRoundTripWithNulls() {
        var serializer = new EntityYsonSerializer<>(Pojo.class);
        var obj = sample();
        obj.nickname = null;
        obj.data = null;
        obj.tags = null;

        var decoded = serializer.deserialize(serializer.serialize(obj));
        assertNull(decoded.nickname);
        assertNull(decoded.data);
        assertNull(decoded.tags);
        assertEquals("flow", decoded.name);
    }

    @Test
    void testTextRoundTrip() {
        var serializer = new EntityYsonSerializer<>(Pojo.class);
        var obj = sample();

        byte[] text = serializer.serialize(obj, false);
        var builder = new YTreeBuilder();
        new YsonParser(text).parseNode(builder);
        var node = builder.build();

        assertPojoEquals(obj, serializer.deserialize(node));
    }

    @Test
    void testAbsentKeyKeepsDefault() {
        var serializer = new EntityYsonSerializer<>(Pojo.class);
        // Build a node containing only "name"; all other fields must stay null.
        var node = new YTreeBuilder()
                .beginMap()
                .key("name").value("only")
                .endMap()
                .build();

        var decoded = serializer.deserialize(node);
        assertEquals("only", decoded.name);
        assertNull(decoded.age);
        assertNull(decoded.nickname);
        assertNull(decoded.tags);
    }

    @Test
    void testExplicitEntityKeySetsNull() {
        var serializer = new EntityYsonSerializer<>(Pojo.class);
        var node = new YTreeBuilder()
                .beginMap()
                .key("name").value("n")
                .key("nickname").entity()
                .endMap()
                .build();

        var decoded = serializer.deserialize(node);
        assertEquals("n", decoded.name);
        assertNull(decoded.nickname);
    }

    @Test
    void testEntityKeyOnPrimitiveKeepsDefault() {
        var serializer = new EntityYsonSerializer<>(WithPrimitives.class);
        // An explicit YSON entity (#) for primitive fields must not attempt to set null (which
        // would fail through reflection); the fields stay at their primitive defaults.
        var node = new YTreeBuilder()
                .beginMap()
                .key("count").entity()
                .key("size").entity()
                .key("active").entity()
                .endMap()
                .build();

        var decoded = serializer.deserialize(node);
        assertEquals(0L, decoded.count);
        assertEquals(0, decoded.size);
        assertFalse(decoded.active);
    }

    @Test
    void testUnknownKeyIgnored() {
        var serializer = new EntityYsonSerializer<>(Pojo.class);
        var node = new YTreeBuilder()
                .beginMap()
                .key("name").value("n")
                .key("unknownColumn").value("whatever")
                .endMap()
                .build();

        var decoded = serializer.deserialize(node);
        assertEquals("n", decoded.name);
    }

    @Test
    void testDeserializeEntityNodeReturnsNull() {
        var serializer = new EntityYsonSerializer<>(Pojo.class);
        var node = new YTreeBuilder().entity().build();
        assertNull(serializer.deserialize(node));
    }
}
