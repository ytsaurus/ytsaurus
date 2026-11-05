package tech.ytsaurus.flow.row;

import org.junit.jupiter.api.Test;
import tech.ytsaurus.core.tables.ColumnValueType;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.flow.test.TTestMessage;
import tech.ytsaurus.typeinfo.TiType;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
        assertEquals(message, payload.get("value", TTestMessage.class));
        assertEquals("1", payload.get("id", String.class));
    }
}
