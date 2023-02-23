package tech.ytsaurus.core.tables;

import java.util.List;

import org.junit.Test;
import tech.ytsaurus.typeinfo.TiType;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeNode;

import static org.junit.Assert.assertEquals;

public class TableSchemaTest {
    private static final TableSchema KEY_VALUE_SCHEMA = new TableSchema.Builder()
            .addKey("key", ColumnValueType.STRING)
            .addValue("value", ColumnValueType.STRING)
            .build();
    private static final YTreeNode KEY_VALUE_SCHEMA_YTREE = YTree.builder()
            .beginAttributes()
                .key("strict").value(true)
                .key("unique_keys").value(true)
            .endAttributes()
            .beginList()
                .beginMap()
                    .key("name").value("key")
                    .key("sort_order").value("ascending")
                    .key("type_v3").beginMap()
                        .key("type_name").value("optional")
                        .key("item").value("string")
                    .endMap()
                .endMap()
                .beginMap()
                    .key("name").value("value")
                    .key("type_v3").beginMap()
                        .key("type_name").value("optional")
                        .key("item").value("string")
                    .endMap()
                .endMap()
            .endList()
            .build();

    private static final TableSchema HASH_COLUMN_SCHEMA = new TableSchema.Builder()
            .add(new ColumnSchema.Builder("h", ColumnValueType.INT64)
                    .setSortOrder(ColumnSortOrder.ASCENDING)
                    .setExpression("hash(...)")
                    .build())
            .addKey("a", ColumnValueType.STRING)
            .addValue("b", ColumnValueType.STRING)
            .addValue("c", ColumnValueType.STRING)
            .build();

    private static final YTreeNode HASH_COLUMN_SCHEMA_YTREE = YTree.builder()
            .beginAttributes()
                .key("strict").value(true)
                .key("unique_keys").value(true)
            .endAttributes()
            .beginList()
                .beginMap()
                    .key("name").value("h")
                    .key("sort_order").value("ascending")
                    .key("expression").value("hash(...)")
                    .key("type_v3").beginMap()
                        .key("type_name").value("optional")
                        .key("item").value("int64")
                    .endMap()
                .endMap()
                .beginMap()
                    .key("name").value("a")
                    .key("sort_order").value("ascending")
                    .key("type_v3").beginMap()
                        .key("type_name").value("optional")
                        .key("item").value("string")
                    .endMap()
                .endMap()
                .beginMap()
                    .key("name").value("b")
                    .key("type_v3").beginMap()
                        .key("type_name").value("optional")
                        .key("item").value("string")
                    .endMap()
                .endMap()
                .beginMap()
                    .key("name").value("c")
                    .key("type_v3").beginMap()
                        .key("type_name").value("optional")
                        .key("item").value("string")
                    .endMap()
                .endMap()
            .endList()
            .build();

    private static final YTreeNode UPDATED_SORT_COLUMN_SCHEMA_YTREE = YTree.builder()
            .beginAttributes()
                .key("strict").value(true)
                .key("unique_keys").value(true)
            .endAttributes()
            .beginList()
                .beginMap()
                    .key("name").value("c")
                    .key("sort_order").value("descending")
                    .key("type_v3").beginMap()
                        .key("type_name").value("optional")
                        .key("item").value("string")
                    .endMap()
                .endMap()
                .beginMap()
                    .key("name").value("b")
                    .key("sort_order").value("ascending")
                    .key("type_v3").beginMap()
                        .key("type_name").value("optional")
                        .key("item").value("string")
                    .endMap()
                .endMap()
                .beginMap()
                    .key("name").value("a")
                    .key("type_v3").beginMap()
                        .key("type_name").value("optional")
                        .key("item").value("string")
                    .endMap()
                .endMap()
            .endList()
            .build();

    @Test
    public void keyValueSchemaToYTree() {
        assertEquals(KEY_VALUE_SCHEMA.toYTree(), KEY_VALUE_SCHEMA_YTREE);
    }

    @Test
    public void keyValueSchemaFromYTree() {
        assertEquals(TableSchema.fromYTree(KEY_VALUE_SCHEMA_YTREE), KEY_VALUE_SCHEMA);
    }

    @Test
    public void hashColumnSchemaToYTree() {
        assertEquals(HASH_COLUMN_SCHEMA.toYTree(), HASH_COLUMN_SCHEMA_YTREE);
    }

    @Test
    public void hashColumnSchemaFromYTree() {
        assertEquals(TableSchema.fromYTree(HASH_COLUMN_SCHEMA_YTREE), HASH_COLUMN_SCHEMA);
    }

    @Test
    public void hashColumnSchemaToWrite() {
        assertEquals(HASH_COLUMN_SCHEMA.toWrite().getColumnNames(), List.of("a", "b", "c"));
    }

    @Test
    public void hashColumnSchemaToLookup() {
        assertEquals(HASH_COLUMN_SCHEMA.toLookup().getColumnNames(), List.of("a"));
    }

    @Test
    public void hashColumnSchemaToDelete() {
        assertEquals(HASH_COLUMN_SCHEMA.toDelete().getColumnNames(), List.of("a"));
    }

    @Test
    public void hashColumnSchemaToKeys() {
        assertEquals(HASH_COLUMN_SCHEMA.toKeys().getColumnNames(), List.of("h", "a"));
    }

    @Test
    public void hashColumnSchemaToValues() {
        assertEquals(HASH_COLUMN_SCHEMA.toValues().getColumnNames(), List.of("b", "c"));
    }

    @Test
    public void testOldColumnDeserialization() {
        assertEquals(
                TableSchema.fromYTree(YTree.builder().beginList().beginMap()
                        .key("name").value("foo")
                        .key("type").value("string")
                        .endMap().endList().build()
                ),
                TableSchema.builder()
                        .addValue("foo", TiType.optional(TiType.string()))
                        .build()
        );

        assertEquals(
                TableSchema.fromYTree(YTree.builder().beginList().beginMap()
                        .key("name").value("foo")
                        .key("type").value("string")
                        .key("required").value(true)
                        .endMap().endList().build()
                ),
                TableSchema.builder()
                        .addValue("foo", TiType.string())
                        .build()
        );
    }

    @Test
    public void testSortBy() {
        var aSortedSchema = new TableSchema.Builder()
            .add(new ColumnSchema.Builder("a", ColumnValueType.STRING)
                    .setSortOrder(ColumnSortOrder.ASCENDING)
                    .build())
            .addKey("b", ColumnValueType.STRING)
            .addValue("c", ColumnValueType.STRING)
            .build();

        var cbSortedSchema = new TableSchema.Builder()
                .add(new ColumnSchema.Builder("c", ColumnValueType.STRING)
                        .setSortOrder(ColumnSortOrder.DESCENDING)
                        .build())
                .addKey("b", ColumnValueType.STRING)
                .addValue("a", ColumnValueType.STRING)
                .build();

        var bcSortedSchema = new TableSchema.Builder()
                .addKey("b", ColumnValueType.STRING)
                .addKey("c", ColumnValueType.STRING)
                .addValue("a", ColumnValueType.STRING)
                .build();

        assertEquals(
                aSortedSchema.toBuilder().sortByColumns(
                    new SortColumn("c", ColumnSortOrder.DESCENDING),
                    new SortColumn("b", ColumnSortOrder.ASCENDING)
                ).build(),
                cbSortedSchema
        );

        assertEquals(
                cbSortedSchema.toBuilder().sortBy("b", "c").build(),
                bcSortedSchema
        );
    }
}
