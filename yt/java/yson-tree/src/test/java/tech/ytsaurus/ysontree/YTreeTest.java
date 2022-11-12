package tech.ytsaurus.ysontree;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Test;

public class YTreeTest {

    @Test
    public void integerNode() {
        YTreeNode node = YTree.builder().value(4243).build();

        Assert.assertTrue(node instanceof YTreeIntegerNode);
        Assert.assertEquals(4243L, node.longValue());
        Assert.assertEquals(4243, node.intValue());
        node = YTree.builder().value(Long.MAX_VALUE).build();
        Assert.assertEquals(Long.MAX_VALUE, node.longValue());
        Assert.assertEquals((int) Long.MAX_VALUE, node.intValue());
    }

    @Test
    public void booleanNode() {
        YTreeNode node = YTree.builder()
                .value(false)
                .build();

        Assert.assertTrue(node instanceof YTreeBooleanNode);
        Assert.assertFalse(node.boolValue());

        node = YTree.builder()
                .value(true)
                .build();

        Assert.assertTrue(node instanceof YTreeBooleanNode);
        Assert.assertTrue(node.boolValue());
    }

    @Test
    public void doubleNode() {
        YTreeNode node = YTree.builder()
                .value(1.4243)
                .build();

        Assert.assertTrue(node instanceof YTreeDoubleNode);
        Assert.assertEquals(1.4243, node.doubleValue(), 0.0001);
        Assert.assertEquals(1.4243F, node.floatValue(), 0.0001);
    }

    @Test
    public void entityNode() {
        YTreeNode node = YTree.builder()
                .entity()
                .build();

        Assert.assertTrue(node instanceof YTreeEntityNode);
    }

    @Test
    public void stringNode() {
        YTreeNode node = YTree.builder()
                .value("hello world")
                .build();

        Assert.assertTrue(node instanceof YTreeStringNode);
        Assert.assertEquals("hello world", node.stringValue());
    }

    @Test
    public void attributesSimple() {
        YTreeNode node = YTree.builder()
                .beginAttributes()
                .key("attr1").value("v1")
                .key("attr12").value(true)
                .endAttributes()
                .value(4243)
                .build();
        Assert.assertTrue(node.containsAttributes());

        Assert.assertTrue(node instanceof YTreeIntegerNode);
        Assert.assertEquals(4243L, node.longValue());
        Assert.assertTrue(node.getAttributeOrThrow("attr1") instanceof YTreeStringNode);
        Assert.assertEquals("v1", node.getAttributeOrThrow("attr1").stringValue());
        Assert.assertTrue(node.getAttributeOrThrow("attr12") instanceof YTreeBooleanNode);
        Assert.assertTrue(node.getAttributeOrThrow("attr12").boolValue());

        Assert.assertSame(node.getAttributeOrThrow("attr1"), node.getAttributes().get("attr1"));
        Assert.assertEquals(Set.of("attr1", "attr12"), node.attributeKeys());
        Assert.assertEquals(2, node.attributeValues().size());
        List<YTreeNode> values = new ArrayList<>(node.attributeValues());
        YTreeNode first = node.getAttributeOrThrow("attr1");
        YTreeNode second = node.getAttributeOrThrow("attr12");
        Assert.assertTrue((values.get(0) == first && values.get(values.size() - 1) == second)
                || (values.get(0) == second && values.get(values.size() - 1) == first));

        Assert.assertTrue(node.containsAttribute("attr12"));
        Assert.assertFalse(node.containsAttribute("attr2"));

        Assert.assertTrue(node.getAttribute("attr12").isPresent());
        Assert.assertSame(node.getAttribute("attr12").get(), node.getAttributeOrThrow("attr12"));

        Assert.assertEquals(node.getAttribute("attr2"), Optional.empty());
    }

    @Test
    public void containsAttributes() {
        YTreeNode node = YTree.builder()
                .beginAttributes()
                .key("attr1").value("v1")
                .key("attr12").value(true)
                .endAttributes()
                .value(4243)
                .build();
        Assert.assertTrue(node.containsAttributes());

        node.clearAttributes();
        Assert.assertFalse(node.containsAttributes());

        YTreeNode empty = YTree.builder().value(4243).build();
        Assert.assertFalse(empty.containsAttributes());
    }

    @Test
    public void containsAttributesOnEmptyNode() {
        YTreeNode empty = YTree.builder().value(4243).build();
        Assert.assertFalse(empty.containsAttributes());
    }

    @Test
    public void attributesInAttributes() {
        YTreeNode node = YTree.builder()
                .beginAttributes()
                .key("key")
                .beginAttributes()
                .key("key1").value(12)
                .key("key2").value("hello")
                .endAttributes()
                .value("value")
                .endAttributes()
                .entity()
                .build();

        Assert.assertTrue(node instanceof YTreeEntityNode);
        Assert.assertEquals(Set.of("key"), node.attributeKeys());
        Assert.assertEquals(Set.of("key1", "key2"), node.getAttributeOrThrow("key").attributeKeys());
        Assert.assertEquals(12L, node.getAttributeOrThrow("key").getAttributeOrThrow("key1").longValue());
        Assert.assertEquals("hello", node.getAttributeOrThrow("key").getAttributeOrThrow("key2").stringValue());
        Assert.assertEquals("value", node.getAttributeOrThrow("key").stringValue());
    }

    @Test
    public void attributesParent() {
        YTreeNode node = YTree.builder()
                .beginAttributes()
                .key("k1")
                .beginMap()
                .key("k1").value("v1")
                .key("k2").value(1.23)
                .endMap()
                .key("k2").value(18)
                .endAttributes()
                .value(148)
                .build();

        Assert.assertTrue(node instanceof YTreeIntegerNode);
        Assert.assertEquals(148L, node.longValue());
        Assert.assertTrue(node.getAttributeOrThrow("k1") instanceof YTreeMapNode);

        Assert.assertTrue(node.getAttributeOrThrow("k1").mapNode().getOrThrow("k1") instanceof YTreeStringNode);
        Assert.assertEquals("v1", node.getAttributeOrThrow("k1").mapNode().getOrThrow("k1").stringValue());

        Assert.assertTrue(node.getAttributeOrThrow("k1").mapNode().getOrThrow("k2") instanceof YTreeDoubleNode);
        Assert.assertEquals(1.23, node.getAttributeOrThrow("k1").mapNode().getOrThrow("k2").doubleValue(), 0.0001);

        Assert.assertTrue(node.getAttributeOrThrow("k2") instanceof YTreeIntegerNode);
        Assert.assertEquals(18L, node.getAttributeOrThrow("k2").longValue());

    }

    @Test
    public void attributesParentInMap() {
        YTreeNode node = YTree.builder()
                .beginAttributes()
                .key("key1")
                .beginMap()
                .key("key2")
                .beginMap()
                .key("key3")
                .value("hello")
                .endMap()
                .endMap()
                .endAttributes()
                .entity()
                .build();

        Assert.assertTrue(node instanceof YTreeEntityNode);
        Assert.assertTrue(node.getAttributeOrThrow("key1") instanceof YTreeMapNode);

        Assert.assertTrue(node.getAttributeOrThrow("key1").mapNode().getOrThrow("key2") instanceof YTreeMapNode);

        Assert.assertTrue(node.getAttributeOrThrow("key1").mapNode()
                .getOrThrow("key2").mapNode().getOrThrow("key3") instanceof YTreeStringNode);
        Assert.assertEquals("hello", node.getAttributeOrThrow("key1").mapNode().getOrThrow("key2")
                .mapNode().getOrThrow("key3").stringValue());
    }

    @Test
    public void attributesParentInList() {
        YTreeNode node = YTree.builder()
                .beginAttributes()
                .key("key")
                .beginList()
                .beginList()
                .value(1)
                .value(2)
                .value(42)
                .endList()
                .value(true)
                .endList()
                .endAttributes()
                .value("123")
                .build();

        Assert.assertTrue(node instanceof YTreeStringNode);
        Assert.assertEquals("123", node.stringValue());

        Assert.assertTrue(node.getAttributeOrThrow("key") instanceof YTreeListNode);

        Assert.assertEquals(2, node.getAttributeOrThrow("key").listNode().size());

        Assert.assertTrue(node.getAttributeOrThrow("key").listNode().get(0) instanceof YTreeListNode);
        Assert.assertEquals(3, node.getAttributeOrThrow("key").listNode().get(0).listNode().size());

        Assert.assertTrue(node.getAttributeOrThrow("key")
            .listNode().get(0).listNode().get(0) instanceof YTreeIntegerNode);
        Assert.assertEquals(1L, node.getAttributeOrThrow("key").listNode().get(0).listNode().get(0).longValue());

        Assert.assertTrue(node.getAttributeOrThrow("key").listNode().get(1) instanceof YTreeBooleanNode);
        Assert.assertTrue(node.getAttributeOrThrow("key").listNode().get(1).boolValue());
    }

    @Test
    public void mapNode() {
        YTreeNode node = YTree.builder()
                .beginMap()
                .key("key1").value(4243)
                .key("key2")
                .beginList()
                .value(1)
                .value(19)
                .endList()
                .endMap()
                .build();

        Assert.assertTrue(node instanceof YTreeMapNode);

        Assert.assertTrue(node.mapNode().getOrThrow("key1") instanceof YTreeIntegerNode);
        Assert.assertEquals(4243L, node.mapNode().getOrThrow("key1").longValue());

        Assert.assertTrue(node.mapNode().getOrThrow("key2") instanceof YTreeListNode);
        Assert.assertEquals(2, node.mapNode().getOrThrow("key2").listNode().size());

        Assert.assertTrue(node.mapNode().getOrThrow("key2").listNode().get(0) instanceof YTreeIntegerNode);
        Assert.assertEquals(1L, node.mapNode().getOrThrow("key2").listNode().get(0).longValue());

        Assert.assertTrue(node.mapNode().getOrThrow("key2").listNode().get(1) instanceof YTreeIntegerNode);
        Assert.assertEquals(19L, node.mapNode().getOrThrow("key2").listNode().get(1).longValue());

        Assert.assertEquals(Set.of("key1", "key2"), node.mapNode().keys());
        Assert.assertEquals(2, node.mapNode().values().size());
        List<YTreeNode> values = new ArrayList<>(node.mapNode().values());
        YTreeNode first = node.mapNode().getOrThrow("key1");
        YTreeNode second = node.mapNode().getOrThrow("key2");
        Assert.assertTrue((values.get(0) == first && values.get(values.size() - 1) == second)
                || (values.get(0) == second && values.get(values.size() - 1) == first));
        Assert.assertSame(node.mapNode().getOrThrow("key1"), node.mapNode().asMap().get("key1"));

        Assert.assertTrue(node.mapNode().containsKey("key1"));
        Assert.assertFalse(node.mapNode().containsKey("key3"));

        Assert.assertTrue(node.mapNode().get("key1").isPresent());
        Assert.assertSame(node.mapNode().get("key1").get(), node.mapNode().getOrThrow("key1"));
    }

    @Test
    public void listNode() {
        YTreeNode node = YTree.builder()
                .beginList()
                .value("hello")
                .value("world")
                .value(4243)
                .beginMap()
                .key("key1").value("value1")
                .key("key2").value("value2")
                .endMap()
                .endList()
                .build();

        Assert.assertTrue(node instanceof YTreeListNode);

        Assert.assertEquals(4, node.listNode().size());

        Assert.assertEquals("hello", node.listNode().get(0).stringValue());

        Assert.assertEquals("world", node.listNode().get(1).stringValue());

        Assert.assertEquals(4243L, node.listNode().get(2).longValue());

        Assert.assertTrue(node.listNode().get(3) instanceof YTreeMapNode);
        Assert.assertEquals("value1", node.listNode().get(3).mapNode().getOrThrow("key1").stringValue());

        Assert.assertEquals("value2", node.listNode().get(3).mapNode().getOrThrow("key2").stringValue());

        List<YTreeNode> list = node.listNode().asList();
        Assert.assertSame(node.listNode().get(0), list.get(0));
    }

    @Test
    public void collectionOfNodesAsValue() {
        YTreeNode node = YTree.builder()
                .value(List.of(YTree.stringNode("hello"), YTree.integerNode(123)))
                .build();

        Assert.assertTrue(node instanceof YTreeListNode);
        Assert.assertEquals(2, node.listNode().size());
        Assert.assertEquals("hello", node.listNode().get(0).stringValue());
        Assert.assertEquals(123L, node.listNode().get(1).longValue());
    }

    @Test
    public void scalarNodeAsValue() {
        YTreeNode node = YTree.builder()
                .beginAttributes()
                .key("key1").value("value1")
                .key("key2").value(true)
                .endAttributes()
                .value(false)
                .build();

        YTreeNode clone = YTree.builder()
                .value(node)
                .build();

        Assert.assertNotSame(node, clone);
        Assert.assertEquals(Set.of("key1", "key2"), clone.attributeKeys());
        Assert.assertFalse(clone.boolValue());
    }

    @Test
    public void listNodeAsValue() {
        YTreeNode node = YTree.builder()
                .beginAttributes()
                .key("key1").value("value1")
                .key("key2").value(true)
                .endAttributes()
                .beginList()
                .value(1)
                .value(2)
                .endList()
                .build();

        YTreeNode clone = YTree.builder()
                .value(node)
                .build();

        Assert.assertNotSame(node, clone);
        Assert.assertEquals(Set.of("key1", "key2"), clone.attributeKeys());
        Assert.assertEquals(List.of(1L, 2L), clone.listNode().asList().stream()
                .map(YTreeNode::longValue)
                .collect(Collectors.toList()));
    }

    @Test
    public void mapNodeAsValue() {
        YTreeNode node = YTree.builder()
                .beginAttributes()
                .key("key1").value("value1")
                .key("key2").value(true)
                .endAttributes()
                .beginMap()
                .key("key11").value(1)
                .key("key22").value(2)
                .endMap()
                .build();

        YTreeNode clone = YTree.builder()
                .value(node)
                .build();

        Assert.assertNotSame(node, clone);
        Assert.assertEquals(Set.of("key1", "key2"), clone.attributeKeys());
        Assert.assertEquals(Set.of("key11", "key22"), clone.mapNode().keys());
    }

    @Test
    public void buildAttributes() {
        Map<String, YTreeNode> attributes = YTree.attributesBuilder()
                .key("key1").value("value1")
                .key("key2").value("value2")
                .buildAttributes();
        Map<String, String> actual = attributes.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().stringValue()));

        Assert.assertEquals(Map.of("key1", "value1", "key2", "value2"), actual);
    }

    @Test(expected = IllegalStateException.class)
    public void withoutValue() {
        YTree.builder().build();
    }

    @Test(expected = IllegalStateException.class)
    public void openedAttributes() {
        YTree.builder()
                .beginAttributes()
                .key("key").value("value")
                .value("12");
    }

    @Test(expected = IllegalStateException.class)
    public void openedAttributesWithoutValue() {
        YTree.builder()
                .beginAttributes()
                .key("key").value("value")
                .build();
    }

    @Test(expected = IllegalStateException.class)
    public void attributesWithoutValue() {
        YTree.builder()
                .beginAttributes()
                .key("key").value("value")
                .endAttributes()
                .build();
    }

    @Test(expected = IllegalStateException.class)
    public void attributesWithoutKey() {
        YTree.builder()
                .beginAttributes()
                .value("value");
    }

    @Test(expected = IllegalStateException.class)
    public void openedMap() {
        YTree.builder()
                .beginMap()
                .key("key").value("value")
                .build();
    }

    @Test(expected = IllegalStateException.class)
    public void mapWithoutKey() {
        YTree.builder()
                .beginMap()
                .value("value");
    }

    @Test(expected = IllegalStateException.class)
    public void openedList() {
        YTree.builder()
                .beginList()
                .build();
    }

    @Test(expected = IllegalStateException.class)
    public void valueValue() {
        YTree.builder()
                .value(12)
                .value(12);
    }

    private void checkHashCodeAndEquals(Object a, Object b, boolean equals) {
        if (equals) {
            Assert.assertEquals(a.hashCode(), b.hashCode());
            Assert.assertEquals(a, b);
        } else {
            Assert.assertNotEquals(a.hashCode(), b.hashCode());
            Assert.assertNotEquals(a, b);
        }
    }

    private void checkSameNodesWithSameAndDifferentAttributes(Function<YTreeBuilder, YTreeBuilder> generateF) {
        YTreeNode n1 = generateF.apply(
                YTree.builder()
                        .beginAttributes()
                        .key("key1").value("value1")
                        .endAttributes()
        ).build();

        YTreeNode n2 = generateF.apply(
                YTree.builder()
                        .beginAttributes()
                        .key("key1").value("value1")
                        .endAttributes()
        ).build();

        checkHashCodeAndEquals(n1, n2, true);

        n1 = generateF.apply(
                YTree.builder()
                        .beginAttributes()
                        .key("key1").value("value1")
                        .endAttributes()
        ).build();

        n2 = generateF.apply(
                YTree.builder()
                        .beginAttributes()
                        .key("key1").value("value2")
                        .endAttributes()
        ).build();

        checkHashCodeAndEquals(n1, n2, false);
    }

    @Test
    public void booleanNodeHashCodeAndEquals() {
        checkHashCodeAndEquals(YTree.booleanNode(false).hashCode(), YTree.booleanNode(false).hashCode(), true);
        checkHashCodeAndEquals(YTree.booleanNode(true).hashCode(), YTree.booleanNode(true).hashCode(), true);
        checkHashCodeAndEquals(YTree.booleanNode(false).hashCode(), YTree.booleanNode(true).hashCode(), false);
        checkSameNodesWithSameAndDifferentAttributes(b -> b.value(true));
    }

    @Test
    public void doubleNodeHashCodeAndEquals() {
        checkHashCodeAndEquals(YTree.doubleNode(42.43), YTree.doubleNode(42.43), true);
        checkHashCodeAndEquals(YTree.doubleNode(42.43), YTree.doubleNode(18.19), false);
        checkSameNodesWithSameAndDifferentAttributes(b -> b.value(42.43));
    }

    @Test
    public void entityNodeHashCodeAndEquals() {
        checkHashCodeAndEquals(YTree.builder().entity().build(), YTree.builder().entity().build(), true);
        checkSameNodesWithSameAndDifferentAttributes(YTreeBuilder::entity);
    }

    @Test
    public void integerNodeHashCodeAndEquals() {
        checkHashCodeAndEquals(YTree.integerNode(42), YTree.integerNode(42), true);
        checkHashCodeAndEquals(YTree.integerNode(42), YTree.integerNode(43), false);
        checkSameNodesWithSameAndDifferentAttributes(b -> b.value(42));
    }

    @Test
    public void listNodeHashCodeAndEquals() {
        checkHashCodeAndEquals(YTree.listBuilder().value(1).value(2).value(3).buildList(),
                YTree.listBuilder().value(1).value(2).value(3).buildList(), true);

        checkHashCodeAndEquals(YTree.listBuilder().value(1).value(2).value(3).buildList(),
                YTree.listBuilder().value(1).value(2).value(4).buildList(), false);

        checkHashCodeAndEquals(YTree.listBuilder().value(1).value(2).value(3).buildList(),
                YTree.listBuilder().value(1).value(2).value(3).value(19).buildList(), false);

        checkSameNodesWithSameAndDifferentAttributes(b -> b.beginList().value(1).value(2).value(3).endList());
    }

    @Test
    public void mapNodeHashCodeAndEquals() {
        checkHashCodeAndEquals(YTree.mapBuilder().key("key1").value("value1").key("key2").value("value2").buildMap(),
                YTree.mapBuilder().key("key1").value("value1").key("key2").value("value2").buildMap(), true);

        checkHashCodeAndEquals(YTree.mapBuilder().key("key1").value("value1").key("key2").value("value2").buildMap(),
                YTree.mapBuilder().key("key1").value("value1").key("key2").value("value3").buildMap(), false);

        checkHashCodeAndEquals(YTree.mapBuilder().key("key1").value("value1").key("key2").value("value2").buildMap(),
                YTree.mapBuilder().key("key1").value("value1").key("key2").value("value2")
                        .key("key3").value("value3").buildMap(), false);

        checkHashCodeAndEquals(YTree.mapBuilder().key("key1").value("value1").key("key2").value("value2").buildMap(),
                YTree.mapBuilder().key("key1").value("value1").key("key2").value(19).buildMap(), false);

        checkSameNodesWithSameAndDifferentAttributes(b -> b.beginMap().key("k1").value("v1").endMap());
    }

    @Test
    public void stringNodeHashCodeAndEquals() {
        checkHashCodeAndEquals(YTree.stringNode("hello"), YTree.stringNode("hello"), true);
        checkHashCodeAndEquals(YTree.stringNode("hello"), YTree.stringNode("world"), false);
        checkSameNodesWithSameAndDifferentAttributes(b -> b.value("hello, world!"));
    }

    @Test
    public void valueWithCallback() {
        YTreeNode expected = YTree.mapBuilder()
                .key("key")
                .beginList()
                .beginList()
                .value(1)
                .value(2)
                .value(3)
                .endList()
                .value(148)
                .endList()
                .buildMap();

        List<YTreeNode> lst = List.of(
                YTree.listBuilder().value(1).value(2).value(3).buildList(),
                YTree.integerNode(148)
        );
        YTreeNode actual = YTree.mapBuilder()
                .key("key").value(lst, YTreeBuilder::value)
                .buildMap();

        Assert.assertEquals(expected, actual);
    }

    @Test
    public void valueObject() {
        YTreeNode expected = YTree.listBuilder()
                .value(1)
                .value(18L)
                .value(true)
                .value("hello")
                .buildList();

        YTreeNode actual = YTree.builder().value(List.of(1, 18L, true, "hello")).build();
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void mergeNodes() {
        YTreeNode node1 = YTree.builder()
                .beginMap()
                .key("key")
                .beginMap()
                .key("a").value("b")
                .endMap()
                .endMap()
                .build();

        YTreeNode node2 = YTree.builder()
                .beginMap()
                .key("key")
                .beginMap()
                .key("c").value("value2")
                .endMap()
                .endMap()
                .build();

        YTreeBuilder builder = YTree.builder();
        YTreeNodeUtils.merge(node1, node2, builder, true);
        YTreeNode actual = builder.build();

        YTreeNode expected = YTree.builder()
                .beginMap()
                .key("key")
                .beginMap()
                .key("a").value("b")
                .key("c").value("value2")
                .endMap()
                .endMap()
                .build();

        Assert.assertEquals(expected, actual);

        YTreeNode node3 = YTree.builder()
                .beginMap()
                .key("key")
                .beginMap()
                .key("a").value("value")
                .endMap()
                .endMap()
                .build();

        builder = YTree.builder();
        YTreeNodeUtils.merge(node1, node3, builder, true);
        actual = builder.build();

        expected = YTree.builder()
                .beginMap()
                .key("key")
                .beginMap()
                .key("a").value("value")
                .endMap()
                .endMap()
                .build();

        Assert.assertEquals(expected, actual);
    }

    @Test
    public void deepCopy() {
        YTreeNode source = YTree.mapBuilder()
                .key("key")
                .beginList()
                .beginList()
                .value(1)
                .value(2)
                .value(3)
                .endList()
                .value(148)
                .endList()
                .buildMap();
        YTreeNode actual = YTree.deepCopy(source);
        Assert.assertNotSame(actual, source);
        Assert.assertEquals(source, actual);

        actual.mapNode().put("key", YTree.integerNode(123));
        Assert.assertNotEquals(source, actual);
    }

    @Test
    public void testToMapBuilder() {
        YTreeMapNode mapNode = YTree.builder()
                .beginAttributes()
                    .key("attr").value(1)
                .endAttributes()
                .beginMap()
                    .key("a").value(1)
                    .key("b").value(2)
                .buildMap();
        YTreeMapNode extendedMapNode = mapNode.toMapBuilder().key("c").value(3).buildMap();

        Assert.assertEquals(
                extendedMapNode,
                YTree.builder()
                        .beginAttributes()
                            .key("attr").value(1)
                        .endAttributes()
                        .beginMap()
                            .key("a").value(1)
                            .key("b").value(2)
                            .key("c").value(3)
                        .buildMap()
        );
    }

    @Test
    public void testToListBuilder() {
        YTreeListNode listNode = YTree.builder()
                .beginAttributes()
                    .key("attr").value(1)
                .endAttributes()
                .beginList()
                    .value(1)
                    .value("a")
                .buildList();
        YTreeListNode extendedListNode = listNode.toListBuilder().value(true).buildList();

        Assert.assertEquals(
                extendedListNode,
                YTree.builder()
                        .beginAttributes()
                            .key("attr").value(1)
                        .endAttributes()
                        .beginList()
                            .value(1)
                            .value("a")
                            .value(true)
                        .buildList()
        );
    }
}
