package tech.ytsaurus.core.cypress;

import java.util.Arrays;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.core.YtTimestamp;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeBuilder;
import tech.ytsaurus.ysontree.YTreeNode;
import tech.ytsaurus.ysontree.YTreeStringNode;

/**
 * @author sankear
 */
public class YPathTest {

    @Test
    public void cypressRoot() {
        Assert.assertEquals(new RichYPath("/", Arrays.asList()), YPath.cypressRoot());
    }

    @Test
    public void objectRoot() {
        Assert.assertEquals(new RichYPath("#1-2-3-4", Arrays.asList()), YPath.objectRoot(new GUID(1, 2, 3, 4)));
    }

    @Test
    public void simple() {
        Assert.assertEquals(new RichYPath("/", Arrays.asList("home", "sankear", "@id")),
                YPath.simple("//home/sankear/@id"));
        Assert.assertEquals(new RichYPath("#1-2-3-4", Arrays.asList("tables", "2015-11-25")),
                YPath.simple("#1-2-3-4/tables/2015-11-25"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void simpleIncorrectRootDesignator() {
        YPath.simple("@hello");
    }

    @Test(expected = IllegalArgumentException.class)
    public void simpleEmptyPathPart() {
        YPath.simple("//home//sankear");
    }

    @Test(expected = IllegalArgumentException.class)
    public void simpleEmptyPathPart2() {
        YPath path = YPath.simple("//one").child("/two");
    }

    @Test(expected = IllegalArgumentException.class)
    public void simpleEmptyPathPart3() {
        YPath path = YPath.simple("//one").child("/two//three");
    }

    @Test(expected = IllegalArgumentException.class)
    public void simpleEndsWithSlash() {
        YPath.simple("//home/sankear/");
    }

    @Test
    public void parent() {
        Assert.assertEquals(new RichYPath("/", Arrays.asList("home", "sankear")),
                YPath.simple("//home/sankear/@id").parent());
        Assert.assertEquals(new RichYPath("#1-2-3-4", Arrays.asList()), YPath.simple("#1-2-3-4/home").parent());
        Assert.assertEquals(new RichYPath("/", Arrays.asList()), YPath.simple("/").parent());
    }

    @Test
    public void name() {
        Assert.assertEquals("home", YPath.simple("#1-2-3-4/home").name());
        Assert.assertEquals("sankear", YPath.simple("//tmp/sankear").name());
    }

    @Test(expected = IllegalStateException.class)
    public void nameOfAttribute() {
        YPath.simple("//tmp/sankear/@attr").name();
    }

    @Test
    public void child() {
        Assert.assertEquals(new RichYPath("/", Arrays.asList("home", "sankear", "tables")),
                YPath.simple("//home/sankear").child("tables"));
        Assert.assertEquals(new RichYPath("/", Arrays.asList("home", "sankear", "tables", "child")),
                YPath.simple("//home/sankear").child("tables/child"));
    }

    @Test
    public void after() {
        Assert.assertEquals(new RichYPath("/", Arrays.asList("home", "sankear", "tables", "after:12")),
                YPath.simple("//home/sankear/tables").after(12));
    }

    @Test
    public void before() {
        Assert.assertEquals(new RichYPath("/", Arrays.asList("home", "sankear", "tables", "before:42")),
                YPath.simple("//home/sankear/tables").before(42));
    }

    @Test
    public void begin() {
        Assert.assertEquals(new RichYPath("/", Arrays.asList("home", "sankear", "tables", "begin")),
                YPath.simple("//home/sankear/tables").begin());
    }

    @Test
    public void end() {
        Assert.assertEquals(new RichYPath("/", Arrays.asList("home", "sankear", "tables", "end")),
                YPath.simple("//home/sankear/tables").end());
    }

    @Test
    public void childIndex() {
        Assert.assertEquals(new RichYPath("/", Arrays.asList("home", "sankear", "tables", "123")),
                YPath.simple("//home/sankear/tables").child(123));
    }

    @Test
    public void attribute() {
        Assert.assertEquals(new RichYPath("/", Arrays.asList("home", "sankear", "@id")),
                YPath.simple("//home/sankear").attribute("id"));
    }

    @Test
    public void all() {
        Assert.assertEquals(new RichYPath("/", Arrays.asList("home", "sankear", "*")),
                YPath.simple("//home/sankear").all());
    }

    @Test
    public void allAttributes() {
        Assert.assertEquals(new RichYPath("/", Arrays.asList("home", "sankear", "@")),
                YPath.simple("//home/sankear").allAttributes());
    }

    @Test
    public void isRoot() {
        Assert.assertTrue(YPath.simple("/").isRoot());
        Assert.assertTrue(YPath.simple("#1-2-3-4").isRoot());
        Assert.assertFalse(YPath.simple("//home").isRoot());
        Assert.assertFalse(YPath.simple("#1-2-3-4/@attr").isRoot());
    }

    @Test
    public void isAttribute() {
        Assert.assertFalse(YPath.simple("/").isAttribute());
        Assert.assertFalse(YPath.simple("#1-2-3-4").isAttribute());
        Assert.assertFalse(YPath.simple("//home").isAttribute());
        Assert.assertTrue(YPath.simple("#1-2-3-4/@attr").isAttribute());
        Assert.assertTrue(YPath.simple("//home/sankear/@id").isAttribute());
    }

    @Test
    public void hasObjectRootDesignator() {
        Assert.assertFalse(YPath.simple("/").hasObjectRootDesignator());
        Assert.assertFalse(YPath.simple("//home/sankear").hasObjectRootDesignator());
        Assert.assertTrue(YPath.simple("#1-2-3-4").hasObjectRootDesignator());
        Assert.assertTrue(YPath.simple("#1-2-3-4/home/@attr").hasObjectRootDesignator());
    }

    @Test
    public void append() {
        Assert.assertEquals(new RichYPath("/", Arrays.asList("home", "sankear", "table")).append(true),
                YPath.simple("//home/sankear/table").append(true));

        Assert.assertEquals(new RichYPath("/", Arrays.asList("home", "sankear", "table")).append(false),
                YPath.simple("//home/sankear/table").append(false));
    }

    @Test
    public void withRange() {
        Assert.assertEquals(new RichYPath("/", Arrays.asList("home", "sankear", "table"))
                        .withRange(RangeLimit.row(1), RangeLimit.row(12)),
                YPath.simple("//home/sankear/table").withRange(RangeLimit.row(1), RangeLimit.row(12)));
    }

    @Test
    public void withColumns() {
        Assert.assertEquals(new RichYPath("/", Arrays.asList("home", "sankear", "table")).withColumns("c1", "c2"),
                YPath.simple("//home/sankear/table").withColumns("c1", "c2"));
    }

    @Test
    public void withTimestamp() {
        Assert.assertEquals(new RichYPath("/", Arrays.asList("home", "sankear", "table")).withTimestamp(123),
                YPath.simple("//home/sankear/table").withTimestamp(123));
    }

    @Test
    public void withYtTimestamp() {
        YPath timestamp = YPath.simple("//some/table")
                .withYtTimestamp(YtTimestamp.valueOf(123));

        Assert.assertEquals(YPath.simple("//some/table").withTimestamp(123), timestamp);
        Assert.assertEquals(YPath.simple("//some/table").withTimestamp(123).toTree(), timestamp.toTree());

        YTreeNode node = YTree.builder()
                .beginAttributes()
                .key("timestamp").value(123)
                .endAttributes()
                .value("//some/table")
                .build();
        Assert.assertEquals(node, timestamp.toTree());

        Assert.assertEquals(YPath.simple("//some/table").withTimestamp(123), YPath.fromTree(node));
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
        Assert.assertEquals(new RichYPath("/", Arrays.asList("home", "sankear", "table")).withSchema(schemaNode),
                YPath.simple("//home/sankear/table").withSchema(schemaNode));
    }

    @Test
    public void toTree() {
        YTreeNode node = YPath.simple("//home/sankear/table").toTree();
        Assert.assertTrue(node instanceof YTreeStringNode);
        Assert.assertEquals("//home/sankear/table", node.stringValue());
    }

    @Test
    public void fromTree() {
        Assert.assertEquals(new RichYPath("/", Arrays.asList("home", "sankear", "table")),
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

        Assert.assertEquals(YPath.simple("//home/sankear/table").withColumns("c1", "c2"),
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

        Assert.assertEquals(YPath.simple("//home/sankear/table")
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

        Assert.assertEquals(YPath.simple("//home/sankear/table").append(true),
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

        Assert.assertEquals(YPath.simple("//home/sankear/table")
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

        Assert.assertEquals(YPath.simple("//home/sankear/table")
                        .primary(true),
                YPath.fromTree(node));

        node = YTree.builder()
                .beginAttributes()
                .key("foreign").value(true)
                .endAttributes()
                .value("//home/sankear/table")
                .build();

        Assert.assertEquals(YPath.simple("//home/sankear/table").foreign(true),
                YPath.fromTree(node));
    }

    @Test
    public void testRenameColumns() {
        YPath expected = YPath.simple("//home/sankear/table")
                .withRenameColumns(Map.of("c1", "newc1", "c2", "newc2"));
        YTreeNode node = expected.toTree();

        Assert.assertEquals(YTree.builder()
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

        Assert.assertEquals(expected, YPath.fromTree(node));
    }

    @Test
    public void withBypassArtifactCache() {
        YPath expected = YPath.simple("//home/sankear/table")
                .withBypassArtifactCache(true);
        YTreeNode node = expected.toTree();

        Assert.assertEquals(YTree.builder()
                .beginAttributes()
                .key("bypass_artifact_cache").value(YTree.booleanNode(true))
                .endAttributes()
                .value("//home/sankear/table")
                .build(), node);

        Assert.assertEquals(expected, YPath.fromTree(node));
    }

    @Test
    public void executable() {
        YPath expected = YPath.simple("//home/sankear/table")
                .withExecutable(true);
        YTreeNode node = expected.toTree();

        Assert.assertEquals(YTree.builder()
                .beginAttributes()
                .key("executable").value(YTree.booleanNode(true))
                .endAttributes()
                .value("//home/sankear/table")
                .build(), node);

        Assert.assertEquals(expected, YPath.fromTree(node));
    }

    @Test
    public void create() {
        YPath expected = YPath.simple("//some/table")
                .create(true);
        YTreeNode node = expected.toTree();

        Assert.assertEquals(YTree.builder()
                .beginAttributes()
                .key("create").value(YTree.booleanNode(true))
                .endAttributes()
                .value("//some/table")
                .build(), node);

        Assert.assertEquals(expected, YPath.fromTree(node));
    }

    @Test
    public void toStringTest() {
        YPath path = YPath.simple("//home/sankear/table");
        Assert.assertEquals("//home/sankear/table", path.toString());
        path = YPath.simple("#1-2-3-4/hello");
        Assert.assertEquals("#1-2-3-4/hello", path.toString());
    }

    @Test
    public void formatTest() {
        Assert.assertEquals(
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
        Assert.assertTrue(path.toString().equals("<\"k1\"=\"v1\";\"k2\"=\"v2\";>//some/table")
                || path.toString().equals("<\"k2\"=\"v2\";\"k1\"=\"v1\";>//some/table"));
        Assert.assertTrue(path.getAdditionalAttributes().toString().equals("{k1=\"v1\", k2=\"v2\"}")
                || path.getAdditionalAttributes().toString().equals("{k2=\"v2\", k1=\"v1\"}"));
    }

    @Test
    public void plusAdditionalAttributeTest() {
        YPath path = YPath.simple("//some/table")
                .withAdditionalAttributes(Map.of("k1", YTree.stringNode("v1")))
                .plusAdditionalAttribute("k2", YTree.stringNode("v2"));

        Assert.assertEquals("<\"k1\"=\"v1\";\"k2\"=\"v2\";>//some/table", path.toString());
        Assert.assertEquals(
                Map.of("k1", YTree.stringNode("v1"), "k2", YTree.stringNode("v2")),
                path.getAdditionalAttributes());
        Assert.assertEquals(path.toStableString(), "<\"k1\"=\"v1\";\"k2\"=\"v2\";>//some/table");
        Assert.assertTrue(path.getAdditionalAttributes().toString().equals("{k1=\"v1\", k2=\"v2\"}")
                || path.getAdditionalAttributes().toString().equals("{k2=\"v2\", k1=\"v1\"}"));
    }

    @Test
    public void plusAdditionalAttributeAsObjectTest() {
        // string
        YPath path = YPath.simple("//some/table")
                .plusAdditionalAttribute("k1", "v1");
        Assert.assertEquals("<\"k1\"=\"v1\";>//some/table", path.toString());
        Assert.assertEquals("{k1=\"v1\"}", path.getAdditionalAttributes().toString());

        // double
        path = YPath.simple("//some/table")
                .plusAdditionalAttribute("k1", 1.01);
        Assert.assertEquals("<\"k1\"=1.01;>//some/table", path.toString());
        Assert.assertEquals("{k1=1.01}", path.getAdditionalAttributes().toString());

        // list
        path = YPath.simple("//some/table")
                .plusAdditionalAttribute("k1", Arrays.asList("v1", "v2"));
        Assert.assertEquals("<\"k1\"=[\"v1\";\"v2\";];>//some/table", path.toString());
        Assert.assertEquals("{k1=[\"v1\";\"v2\";]}", path.getAdditionalAttributes().toString());

        // map
        path = YPath.simple("//some/table")
                .plusAdditionalAttribute("k1", Map.of("kk1", 1.01, "kk2", 2.02));
        Assert.assertEquals("<\"k1\"={\"kk1\"=1.01;\"kk2\"=2.02;};>//some/table", path.toString());
        Assert.assertEquals("{k1={\"kk1\"=1.01;\"kk2\"=2.02;}}", path.getAdditionalAttributes().toString());

        // list inside map (complex one)
        path = YPath.simple("//some/table")
                .plusAdditionalAttribute("k1", Map.of("list", Arrays.asList("v1", "v2")));
        Assert.assertEquals("<\"k1\"={\"list\"=[\"v1\";\"v2\";];};>//some/table", path.toString());
        Assert.assertEquals("{k1={\"list\"=[\"v1\";\"v2\";];}}", path.getAdditionalAttributes().toString());
    }
}
