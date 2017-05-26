package ru.yandex.yt.ytclient.tables;

import org.junit.Test;

import ru.yandex.yt.ytclient.ytree.YTreeNode;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;

public class TableSchemaTest {
    private static final TableSchema KEY_VALUE_SCHEMA = new TableSchema.Builder()
            .addKey("key", ColumnValueType.STRING)
            .addValue("value", ColumnValueType.STRING)
            .build();
    private static final YTreeNode KEY_VALUE_SCHEMA_YTREE = YTreeNode.parseString(
            "<\"strict\"=%true;\"unique_keys\"=%true>[{\"name\"=\"key\";\"type\"=\"string\";\"sort_order\"=\"ascending\"};{\"name\"=\"value\";\"type\"=\"string\"}]");

    private static final TableSchema HASH_COLUMN_SCHEMA = new TableSchema.Builder()
            .add(new ColumnSchema.Builder("h", ColumnValueType.INT64)
                    .setSortOrder(ColumnSortOrder.ASCENDING)
                    .setExpression("hash(...)")
                    .build())
            .addKey("a", ColumnValueType.STRING)
            .addValue("b", ColumnValueType.STRING)
            .addValue("c", ColumnValueType.STRING)
            .build();
    private static final YTreeNode HASH_COLUMN_SCHEMA_YTREE = YTreeNode.parseString(
            "<\"strict\"=%true;\"unique_keys\"=%true>[{\"name\"=\"h\";\"type\"=\"int64\";\"sort_order\"=\"ascending\";\"expression\"=\"hash(...)\"};{\"name\"=\"a\";\"type\"=\"string\";\"sort_order\"=\"ascending\"};{\"name\"=\"b\";\"type\"=\"string\"};{\"name\"=\"c\";\"type\"=\"string\"}]");

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
}
