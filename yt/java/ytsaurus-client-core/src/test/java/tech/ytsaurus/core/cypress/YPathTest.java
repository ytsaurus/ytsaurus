package tech.ytsaurus.core.cypress;

import java.util.Arrays;
import java.util.Map;

import org.junit.jupiter.api.Test;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.core.YtTimestamp;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeBuilder;
import tech.ytsaurus.ysontree.YTreeNode;
import tech.ytsaurus.ysontree.YTreeStringNode;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author sankear
 */
public class YPathTest {

    @Test
    public void cypressRoot() {
        assertEquals(new RichYPath("/", Arrays.asList()), YPath.cypressRoot());
    }

    @Test
    public void objectRoot() {
        assertEquals(new RichYPath("#1-2-3-4", Arrays.asList()), YPath.objectRoot(new GUID(1, 2, 3, 4)));
    }

    @Test
    public void simple() {
        assertEquals(new RichYPath("/", Arrays.asList("home", "sankear", "@id")),
                YPath.simple("//home/sankear/@id"));
        assertEquals(new RichYPath("#1-2-3-4", Arrays.asList("tables", "2015-11-25")),
                YPath.simple("#1-2-3-4/tables/2015-11-25"));
    }

    @Test
    public void simpleIncorrectRootDesignator() {
        assertThrows(IllegalArgumentException.class, () -> YPath.simple("@hello"));
    }

    @Test
    public void simpleEmptyPathPart() {
        assertThrows(IllegalArgumentException.class, () -> YPath.simple("//home//sankear"));
    }

    @Test
    public void simpleEmptyPathPart2() {
        assertThrows(IllegalArgumentException.class, () -> YPath.simple("//one").child("/two"));
    }

    @Test
    public void simpleEmptyPathPart3() {
        assertThrows(IllegalArgumentException.class, () -> YPath.simple("//one").child("/two//three"));
    }

    @Test
    public void simpleEndsWithSlash() {
        assertThrows(IllegalArgumentException.class, () -> YPath.simple("//home/sankear/"));
    }

    @Test
    public void parent() {
        assertEquals(new RichYPath("/", Arrays.asList("home", "sankear")),
                YPath.simple("//home/sankear/@id").parent());
        assertEquals(new RichYPath("#1-2-3-4", Arrays.asList()), YPath.simple("#1-2-3-4/home").parent());
        assertEquals(new RichYPath("/", Arrays.asList()), YPath.simple("/").parent());
    }

    @Test
    public void name() {
        assertEquals("home", YPath.simple("#1-2-3-4/home").name());
        assertEquals("sankear", YPath.simple("//tmp/sankear").name());
    }

    @Test
    public void nameOfAttribute() {
        assertThrows(IllegalStateException.class, () -> YPath.simple("//tmp/sankear/@attr").name());
    }

    @Test
    public void child() {
        assertEquals(new RichYPath("/", Arrays.asList("home", "sankear", "tables")),
                YPath.simple("//home/sankear").child("tables"));
        assertEquals(new RichYPath("/", Arrays.asList("home", "sankear", "tables", "child")),
                YPath.simple("//home/sankear").child("tables/child"));
    }

    @Test
    public void after() {
        assertEquals(new RichYPath("/", Arrays.asList("home", "sankear", "tables", "after:12")),
                YPath.simple("//home/sankear/tables").after(12));
    }

    @Test
    public void before() {
        assertEquals(new RichYPath("/", Arrays.asList("home", "sankear", "tables", "before:42")),
                YPath.simple("//home/sankear/tables").before(42));
    }

    @Test
    public void begin() {
        assertEquals(new RichYPath("/", Arrays.asList("home", "sankear", "tables", "begin")),
                YPath.simple("//home/sankear/tables").begin());
    }

    @Test
    public void end() {
        assertEquals(new RichYPath("/", Arrays.asList("home", "sankear", "tables", "end")),
                YPath.simple("//home/sankear/tables").end());
    }

    @Test
    public void childIndex() {
        assertEquals(new RichYPath("/", Arrays.asList("home", "sankear", "tables", "123")),
                YPath.simple("//home/sankear/tables").child(123));
    }

    @Test
    public void attribute() {
        assertEquals(new RichYPath("/", Arrays.asList("home", "sankear", "@id")),
                YPath.simple("//home/sankear").attribute("id"));
    }

    @Test
    public void all() {
        assertEquals(new RichYPath("/", Arrays.asList("home", "sankear", "*")),
                YPath.simple("//home/sankear").all());
    }

    @Test
    public void allAttributes() {
        assertEquals(new RichYPath("/", Arrays.asList("home", "sankear", "@")),
                YPath.simple("//home/sankear").allAttributes());
    }

    @Test
    public void isRoot() {
        assertTrue(YPath.simple("/").isRoot());
        assertTrue(YPath.simple("#1-2-3-4").isRoot());
        assertFalse(YPath.simple("//home").isRoot());
        assertFalse(YPath.simple("#1-2-3-4/@attr").isRoot());
    }

    @Test
    public void isAttribute() {
        assertFalse(YPath.simple("/").isAttribute());
        assertFalse(YPath.simple("#1-2-3-4").isAttribute());
        assertFalse(YPath.simple("//home").isAttribute());
        assertTrue(YPath.simple("#1-2-3-4/@attr").isAttribute());
        assertTrue(YPath.simple("//home/sankear/@id").isAttribute());
    }

    @Test
    public void hasObjectRootDesignator() {
        assertFalse(YPath.simple("/").hasObjectRootDesignator());
        assertFalse(YPath.simple("//home/sankear").hasObjectRootDesignator());
        assertTrue(YPath.simple("#1-2-3-4").hasObjectRootDesignator());
        assertTrue(YPath.simple("#1-2-3-4/home/@attr").hasObjectRootDesignator());
    }

    @Test
    public void append() {
        assertEquals(new RichYPath("/", Arrays.asList("home", "sankear", "table")).append(true),
                YPath.simple("//home/sankear/table").append(true));

        assertEquals(new RichYPath("/", Arrays.asList("home", "sankear", "table")).append(false),
                YPath.simple("//home/sankear/table").append(false));
    }

    @Test
    public void withRange() {
        assertEquals(new RichYPath("/", Arrays.asList("home", "sankear", "table"))
                        .withRange(RangeLimit.row(1), RangeLimit.row(12)),
                YPath.simple("//home/sankear/table").withRange(RangeLimit.row(1), RangeLimit.row(12)));
    }

    @Test
    public void withColumns() {
        assertEquals(new RichYPath("/", Arrays.asList("home", "sankear", "table")).withColumns("c1", "c2"),
                YPath.simple("//home/sankear/table").withColumns("c1", "c2"));
    }

    @Test
    public void withTimestamp() {
        assertEquals(new RichYPath("/", Arrays.asList("home", "sankear", "table")).withTimestamp(123),
                YPath.simple("//home/sankear/table").withTimestamp(123));
    }

    @Test
    public void withYtTimestamp() {
        YPath timestamp = YPath.simple("//some/table")
                .withYtTimestamp(YtTimestamp.valueOf(123));

        assertEquals(YPath.simple("//some/table").withTimestamp(123), timestamp);
        assertEquals(YPath.simple("//some/table").withTimestamp(123).toTree(), timestamp.toTree());

        YTreeNode node = YTree.builder()
                .beginAttributes()
                .key("timestamp").value(123)
                .endAttributes()
                .value("//some/table")
                .build();
        assertEquals(node, timestamp.toTree());

        assertEquals(YPath.simple("//some/table").withTimestamp(123), YPath.fromTree(node));
    }

    @Test
    public void withSchema() {
        YTreeBuilder builder = YTree.listBuilder();
        builder.beginMap()
                .key("name").value("column1")
                .key("type").value("id")
                .key("sort_order").value("asc")
                .endMap();
        YTreeBuilder schemaBuilder = YTree.builder().beginAttributes()
                .key("strict").value(true)
                .key("unique_keys").value(true)
                .endAttributes()
                .value(builder.buildList());
        YTreeNode schemaNode = schemaBuilder.build();
        assertEquals(new RichYPath("/", Arrays.asList("home", "sankear", "table")).withSchema(schemaNode),
                YPath.simple("//home/sankear/table").withSchema(schemaNode));
    }

    @Test
    public void toTree() {
        YTreeNode node = YPath.simple("//home/sankear/table").toTree();
        assertTrue(node instanceof YTreeStringNode);
        assertEquals("//home/sankear/table", node.stringValue());
    }

    @Test
    public void fromTree() {
        assertEquals(new RichYPath("/", Arrays.asList("home", "sankear", "table")),
                YPath.fromTree(YTree.stringNode("//home/sankear/table")));

        YTreeNode node = YTree.builder()
                .beginAttributes()
                .key("columns")
                .beginList()
                .value("c1")
                .value("c2")
                .endList()
                .endAttributes()
                .value("//home/sankear/table")
                .build();

        assertEquals(YPath.simple("//home/sankear/table").withColumns("c1", "c2"),
                YPath.fromTree(node));

        node = YTree.builder()
                .beginAttributes()
                .key("ranges")
                .beginList()
                .beginMap()
                .key("lower_limit")
                .beginMap()
                .key("row_index").value(1)
                .endMap()
                .key("upper_limit")
                .beginMap()
                .key("row_index").value(12)
                .key("offset").value(143)
                .key("key")
                .beginList()
                .value("key1")
                .value("key2")
                .endList()
                .endMap()
                .endMap()
                .endList()
                .endAttributes()
                .value("//home/sankear/table")
                .build();

        assertEquals(YPath.simple("//home/sankear/table")
                        .withRange(RangeLimit.row(1), RangeLimit.builder()
                                .setKey(YTree.stringNode("key1"), YTree.stringNode("key2"))
                                .setRowIndex(12)
                                .setOffset(143)
                                .build()),
                YPath.fromTree(node));

        node = YTree.builder()
                .beginAttributes()
                .key("append").value(true)
                .endAttributes()
                .value("//home/sankear/table")
                .build();

        assertEquals(YPath.simple("//home/sankear/table").append(true),
                YPath.fromTree(node));

        node = YTree.builder()
                .beginAttributes()
                .key("ranges")
                .beginList()
                .beginMap()
                .key("lower_limit")
                .beginMap()
                .key("row_index").value(1)
                .endMap()
                .key("upper_limit")
                .beginMap()
                .key("row_index").value(12)
                .key("offset").value(143)
                .key("key")
                .beginList()
                .value("key1")
                .value("key2")
                .endList()
                .endMap()
                .endMap()
                .beginMap()
                .key("exact")
                .beginMap()
                .key("key")
                .beginList()
                .value("exact_value")
                .endList()
                .endMap()
                .endMap()
                .endList()
                .key("columns")
                .beginList()
                .value("c1")
                .value("c2")
                .endList()
                .key("sorted_by")
                .beginList()
                .value("c3")
                .value("c4")
                .endList()
                .key("timestamp").value(123L)
                .endAttributes()
                .value("//home/sankear/table")
                .build();

        assertEquals(YPath.simple("//home/sankear/table")
                        .withRange(RangeLimit.row(1), RangeLimit.builder()
                                .setKey(YTree.stringNode("key1"), YTree.stringNode("key2"))
                                .setRowIndex(12)
                                .setOffset(143)
                                .build())
                        .withExact(RangeLimit.key(YTree.stringNode("exact_value")))
                        .withColumns("c1", "c2")
                        .sortedBy("c3", "c4")
                        .withTimestamp(123L),
                YPath.fromTree(node));

        node = YTree.builder()
                .beginAttributes()
                .key("primary").value(true)
                .endAttributes()
                .value("//home/sankear/table")
                .build();

        assertEquals(YPath.simple("//home/sankear/table")
                        .primary(true),
                YPath.fromTree(node));

        node = YTree.builder()
                .beginAttributes()
                .key("foreign").value(true)
                .endAttributes()
                .value("//home/sankear/table")
                .build();

        assertEquals(YPath.simple("//home/sankear/table").foreign(true),
                YPath.fromTree(node));
    }

    @Test
    public void testRenameColumns() {
        YPath expected = YPath.simple("//home/sankear/table")
                .withRenameColumns(Map.of("c1", "newc1", "c2", "newc2"));
        YTreeNode node = expected.toTree();

        assertEquals(YTree.builder()
                .beginAttributes()
                .key("rename_columns").value(
                        YTree.mapBuilder()
                                .key("c1").value("newc1")
                                .key("c2").value("newc2")
                                .buildMap()
                )
                .endAttributes()
                .value("//home/sankear/table")
                .build(), node);

        assertEquals(expected, YPath.fromTree(node));
    }

    @Test
    public void withBypassArtifactCache() {
        YPath expected = YPath.simple("//home/sankear/table")
                .withBypassArtifactCache(true);
        YTreeNode node = expected.toTree();

        assertEquals(YTree.builder()
                .beginAttributes()
                .key("bypass_artifact_cache").value(YTree.booleanNode(true))
                .endAttributes()
                .value("//home/sankear/table")
                .build(), node);

        assertEquals(expected, YPath.fromTree(node));
    }

    @Test
    public void executable() {
        YPath expected = YPath.simple("//home/sankear/table")
                .withExecutable(true);
        YTreeNode node = expected.toTree();

        assertEquals(YTree.builder()
                .beginAttributes()
                .key("executable").value(YTree.booleanNode(true))
                .endAttributes()
                .value("//home/sankear/table")
                .build(), node);

        assertEquals(expected, YPath.fromTree(node));
    }

    @Test
    public void create() {
        YPath expected = YPath.simple("//some/table")
                .create(true);
        YTreeNode node = expected.toTree();

        assertEquals(YTree.builder()
                .beginAttributes()
                .key("create").value(YTree.booleanNode(true))
                .endAttributes()
                .value("//some/table")
                .build(), node);

        assertEquals(expected, YPath.fromTree(node));
    }

    @Test
    public void toStringTest() {
        YPath path = YPath.simple("//home/sankear/table");
        assertEquals("//home/sankear/table", path.toString());
        path = YPath.simple("#1-2-3-4/hello");
        assertEquals("#1-2-3-4/hello", path.toString());
    }

    @Test
    public void formatTest() {
        assertEquals(
                "<\"format\"=\"yson\";>//some/table",
                YPath.simple("//some/table").withFormat("yson").toString()
        );
    }

    @Test
    public void additionAttributesTest() {
        YPath path = YPath.simple("//some/table")
                .withAdditionalAttributes(Map.of(
                        "k1", YTree.stringNode("v1"),
                        "k2", YTree.stringNode("v2")
                ));
        assertTrue(path.toString().equals("<\"k1\"=\"v1\";\"k2\"=\"v2\";>//some/table")
                || path.toString().equals("<\"k2\"=\"v2\";\"k1\"=\"v1\";>//some/table"));
        assertTrue(path.getAdditionalAttributes().toString().equals("{k1=\"v1\", k2=\"v2\"}")
                || path.getAdditionalAttributes().toString().equals("{k2=\"v2\", k1=\"v1\"}"));
    }

    @Test
    public void plusAdditionalAttributeTest() {
        YPath path = YPath.simple("//some/table")
                .withAdditionalAttributes(Map.of("k1", YTree.stringNode("v1")))
                .plusAdditionalAttribute("k2", YTree.stringNode("v2"));

        assertEquals("<\"k1\"=\"v1\";\"k2\"=\"v2\";>//some/table", path.toString());
        assertEquals(
                Map.of("k1", YTree.stringNode("v1"), "k2", YTree.stringNode("v2")),
                path.getAdditionalAttributes());
        assertEquals("<\"k1\"=\"v1\";\"k2\"=\"v2\";>//some/table", path.toStableString());
        assertTrue(path.getAdditionalAttributes().toString().equals("{k1=\"v1\", k2=\"v2\"}")
                || path.getAdditionalAttributes().toString().equals("{k2=\"v2\", k1=\"v1\"}"));
    }

    @Test
    public void plusAdditionalAttributeAsObjectTest() {
        // string
        YPath path = YPath.simple("//some/table")
                .plusAdditionalAttribute("k1", "v1");
        assertEquals("<\"k1\"=\"v1\";>//some/table", path.toString());
        assertEquals("{k1=\"v1\"}", path.getAdditionalAttributes().toString());

        // double
        path = YPath.simple("//some/table")
                .plusAdditionalAttribute("k1", 1.01);
        assertEquals("<\"k1\"=1.01;>//some/table", path.toString());
        assertEquals("{k1=1.01}", path.getAdditionalAttributes().toString());

        // list
        path = YPath.simple("//some/table")
                .plusAdditionalAttribute("k1", Arrays.asList("v1", "v2"));
        assertEquals("<\"k1\"=[\"v1\";\"v2\";];>//some/table", path.toString());
        assertEquals("{k1=[\"v1\";\"v2\";]}", path.getAdditionalAttributes().toString());

        // map
        path = YPath.simple("//some/table")
                .plusAdditionalAttribute("k1", Map.of("kk1", 1.01, "kk2", 2.02));
        assertEquals("<\"k1\"={\"kk1\"=1.01;\"kk2\"=2.02;};>//some/table", path.toString());
        assertEquals("{k1={\"kk1\"=1.01;\"kk2\"=2.02;}}", path.getAdditionalAttributes().toString());

        // list inside map (complex one)
        path = YPath.simple("//some/table")
                .plusAdditionalAttribute("k1", Map.of("list", Arrays.asList("v1", "v2")));
        assertEquals("<\"k1\"={\"list\"=[\"v1\";\"v2\";];};>//some/table", path.toString());
        assertEquals("{k1={\"list\"=[\"v1\";\"v2\";];}}", path.getAdditionalAttributes().toString());
    }

    @Test
    public void relativeYPathTest() {
        assertEquals(new RichYPath("", Arrays.asList("tasks", "task", "job_count")),
                YPath.relative("/tasks/task/job_count"));
    }

    @Test
    public void incorrectRelativeYPathTest() {
        assertThrows(IllegalArgumentException.class, () -> YPath.relative("//tasks/task/job_count"));
    }
}
