package tech.ytsaurus.skiff;

import java.util.List;

import org.junit.Test;
import tech.ytsaurus.ysontree.YTreeNode;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static tech.ytsaurus.ysontree.YTree.listBuilder;
import static tech.ytsaurus.ysontree.YTree.mapBuilder;

public class SkiffSchemaTest {
    @Test
    public void testSchemeEqual() {
        var schema1 = SkiffSchema.tuple(
                List.of(SkiffSchema.simpleType(WireType.INT_64).setName("num"),
                        SkiffSchema.simpleType(WireType.STRING_32).setName("str"),
                        SkiffSchema.variant8(List.of(
                                        SkiffSchema.nothing(),
                                        SkiffSchema.simpleType(WireType.DOUBLE)))
                                .setName("pointNum")));
        var schema2 = SkiffSchema.tuple(
                List.of(SkiffSchema.simpleType(WireType.INT_64).setName("num"),
                        SkiffSchema.simpleType(WireType.STRING_32).setName("str"),
                        SkiffSchema.variant8(List.of(
                                        SkiffSchema.nothing(),
                                        SkiffSchema.simpleType(WireType.DOUBLE)))
                                .setName("pointNum")));

        assertEquals(schema1, schema2);
    }

    @Test
    public void testSchemeDifferent() {
        var schema1 = SkiffSchema.tuple(
                List.of(SkiffSchema.simpleType(WireType.INT_64).setName("num"),
                        SkiffSchema.simpleType(WireType.STRING_32).setName("str"),
                        SkiffSchema.variant8(List.of(
                                        SkiffSchema.nothing(),
                                        SkiffSchema.simpleType(WireType.INT_16)))
                                .setName("pointNum")));
        var schema2 = SkiffSchema.tuple(
                List.of(SkiffSchema.simpleType(WireType.INT_64).setName("num"),
                        SkiffSchema.simpleType(WireType.STRING_32).setName("str"),
                        SkiffSchema.repeatedVariant8(List.of(
                                        SkiffSchema.nothing(),
                                        SkiffSchema.simpleType(WireType.INT_16)))
                                .setName("pointNum")));

        assertNotEquals(schema1, schema2);
    }

    @Test
    public void testToYTree() {
        var schema = SkiffSchema.tuple(
                List.of(SkiffSchema.simpleType(WireType.INT_64).setName("num"),
                        SkiffSchema.simpleType(WireType.STRING_32).setName("str"),
                        SkiffSchema.variant8(List.of(
                                        SkiffSchema.nothing(),
                                        SkiffSchema.simpleType(WireType.INT_16)))
                                .setName("pointNum")));

        YTreeNode ytree = mapBuilder()
                .key("wire_type").value("tuple")
                .key("children").value(
                        listBuilder()
                                .value(mapBuilder()
                                        .key("wire_type").value("int64")
                                        .key("name").value("num").endMap().build())
                                .value(mapBuilder()
                                        .key("wire_type").value("string32")
                                        .key("name").value("str").endMap().build())
                                .value(mapBuilder()
                                        .key("wire_type").value("variant8")
                                        .key("name").value("pointNum")
                                        .key("children").value(listBuilder()
                                                .value(mapBuilder()
                                                        .key("wire_type").value("nothing")
                                                        .endMap().build())
                                                .value(mapBuilder()
                                                        .key("wire_type").value("int16")
                                                        .endMap().build())
                                                .endList().build())
                                        .endMap().build())
                                .endList().build())
                .endMap().build();

        assertEquals(schema.toYTree(), ytree);
    }
}
