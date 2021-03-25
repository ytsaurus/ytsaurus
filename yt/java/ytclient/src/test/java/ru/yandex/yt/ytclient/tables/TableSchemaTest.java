package ru.yandex.yt.ytclient.tables;

import org.junit.Test;

import ru.yandex.inside.yt.kosher.impl.ytree.builder.YTree;
import ru.yandex.inside.yt.kosher.ytree.YTreeNode;
import ru.yandex.type_info.TiType;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;

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

    @Test
    public void keyValueSchemaToYTree() {
        assertThat(KEY_VALUE_SCHEMA.toYTree(), is(KEY_VALUE_SCHEMA_YTREE));
    }

    @Test
    public void keyValueSchemaFromYTree() {
        assertThat(TableSchema.fromYTree(KEY_VALUE_SCHEMA_YTREE), is(KEY_VALUE_SCHEMA));
    }

    @Test
    public void hashColumnSchemaToYTree() {
        assertThat(HASH_COLUMN_SCHEMA.toYTree(), is(HASH_COLUMN_SCHEMA_YTREE));
    }

    @Test
    public void hashColumnSchemaFromYTree() {
        assertThat(TableSchema.fromYTree(HASH_COLUMN_SCHEMA_YTREE), is(HASH_COLUMN_SCHEMA));
    }

    @Test
    public void hashColumnSchemaToWrite() {
        assertThat(HASH_COLUMN_SCHEMA.toWrite().getColumnNames(), contains("a", "b", "c"));
    }

    @Test
    public void hashColumnSchemaToLookup() {
        assertThat(HASH_COLUMN_SCHEMA.toLookup().getColumnNames(), contains("a"));
    }

    @Test
    public void hashColumnSchemaToDelete() {
        assertThat(HASH_COLUMN_SCHEMA.toDelete().getColumnNames(), contains("a"));
    }

    @Test
    public void hashColumnSchemaToKeys() {
        assertThat(HASH_COLUMN_SCHEMA.toKeys().getColumnNames(), contains("h", "a"));
    }

    @Test
    public void hashColumnSchemaToValues() {
        assertThat(HASH_COLUMN_SCHEMA.toValues().getColumnNames(), contains("b", "c"));
    }

    @Test
    public void testOldColumnDeserialization() {
        assertThat(
                TableSchema.fromYTree(YTree.builder().beginList().beginMap()
                        .key("name").value("foo")
                        .key("type").value("string")
                        .endMap().endList().build()
                ),
                is(TableSchema.builder()
                        .setUniqueKeys(false)
                        .addValue("foo", TiType.optional(TiType.string()))
                        .build()
                )
        );

        assertThat(
                TableSchema.fromYTree(YTree.builder().beginList().beginMap()
                        .key("name").value("foo")
                        .key("type").value("string")
                        .key("required").value(true)
                        .endMap().endList().build()
                ),
                is(TableSchema.builder()
                        .setUniqueKeys(false)
                        .addValue("foo", TiType.string())
                        .build()
                )
        );
    }
}
