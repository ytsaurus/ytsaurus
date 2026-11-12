package tech.ytsaurus.flow.row;

import org.junit.jupiter.api.Test;
import tech.ytsaurus.core.tables.ColumnValueType;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.flow.row.codec.ProtoWirePayloadCodec;
import tech.ytsaurus.flow.test.TTestMessage;
import tech.ytsaurus.typeinfo.TiType;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class PayloadBuilderTest {

    @Test
    public void testDifferentOrders() {
        TableSchema tableSchema = TableSchema.builder()
                .addValue("col1", TiType.string())
                .addValue("col2", TiType.int64())
                .addValue("col3", TiType.bool())
                .build();
        var builder = new PayloadBuilder(tableSchema);
        builder.set("col1", "value1");
        builder.set("col2", 2);
        builder.set("col3", true);
        var row1 = builder.finish();
        builder.set("col2", 2);
        builder.set("col3", true);
        builder.set("col1", "value1");
        var row2 = builder.finish();
        assertEquals(row1, row2);
    }

    @Test
    public void testNonAllColumns() {
        TableSchema tableSchema = TableSchema.builder()
                .addValue("col1", TiType.string())
                .addValue("col2", TiType.int64())
                .addValue("col3", TiType.bool())
                .build();
        var payloadBuilder = new PayloadBuilder(tableSchema);
        var row = payloadBuilder
                .set("col1", "value1")
                .set("col2", 2L)
                .finish();
        var row2 = payloadBuilder
                .set("col1", "value1")
                .set("col2", 2L)
                .finish();
        assertEquals(row, row2);
    }

    @Test
    public void testCompositeListRoundTrip() {
        var schema = TableSchema.builder()
                .addValue("tokens", TiType.list(TiType.string()))
                .build();
        var tokens = new String[]{"a", "b"};
        var payload = new PayloadBuilder(schema).set("tokens", tokens).finish();

        var codec = ProtoWirePayloadCodec.INSTANCE.codecFor(schema);
        var decoded = codec.decode(codec.encode(payload));

        assertEquals(ColumnValueType.COMPOSITE, payload.getRow().getValues().get(0).getType());
        assertEquals(ColumnValueType.COMPOSITE, decoded.getRow().getValues().get(0).getType());
        assertArrayEquals(tokens, decoded.get("tokens", String[].class));
    }

    @Test
    public void testCompositeListEquality() {
        var schema = TableSchema.builder()
                .addValue("tokens", TiType.list(TiType.string()))
                .build();
        var first = new PayloadBuilder(schema).set("tokens", new String[]{"a"}).finish();
        var same = new PayloadBuilder(schema).set("tokens", new String[]{"a"}).finish();
        var different = new PayloadBuilder(schema).set("tokens", new String[]{"b"}).finish();

        assertEquals(ColumnValueType.COMPOSITE, first.getRow().getValues().get(0).getType());
        assertEquals(first, same);
        assertEquals(first.hashCode(), same.hashCode());
        assertNotEquals(first, different);
        assertNotEquals(different, first);
    }

    @Test
    public void testNullOptionalList() {
        var schema = TableSchema.builder()
                .addValue("tokens", TiType.optional(TiType.list(TiType.string())))
                .build();
        var payload = new PayloadBuilder(schema)
                .set("tokens", new String[]{"a"})
                .set("tokens", null)
                .finish();

        assertEquals(ColumnValueType.NULL, payload.getRow().getValues().get(0).getType());
        assertNull(payload.get("tokens", String[].class));
    }

    @Test
    public void testProtoAnyField() {
        var message = TTestMessage.newBuilder()
                .setId("1")
                .setTime(1714215000L)
                .setCount(1)
                .setValue(1.1)
                .setIsValue(true)
                .build();
        TableSchema tableSchema = TableSchema.builder()
                .addValue("id", ColumnValueType.STRING)
                .addValue("value", ColumnValueType.ANY)
                .build();
        var payloadBuilder = new PayloadBuilder(tableSchema);
        var payload = payloadBuilder
                .set("id", "1")
                .set("value", message)
                .finish();
        assertEquals(ColumnValueType.ANY, payload.getRow().getValues().get(1).getType());
        assertEquals(message, payload.get("value", TTestMessage.class));
        assertEquals("1", payload.get("id", String.class));
    }
}
