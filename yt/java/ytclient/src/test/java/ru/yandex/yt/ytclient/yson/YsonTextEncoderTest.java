package ru.yandex.yt.ytclient.yson;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import ru.yandex.yt.ytclient.ytree.YTreeBooleanNode;
import ru.yandex.yt.ytclient.ytree.YTreeDoubleNode;
import ru.yandex.yt.ytclient.ytree.YTreeEntityNode;
import ru.yandex.yt.ytclient.ytree.YTreeInt64Node;
import ru.yandex.yt.ytclient.ytree.YTreeListNode;
import ru.yandex.yt.ytclient.ytree.YTreeMapNode;
import ru.yandex.yt.ytclient.ytree.YTreeNode;
import ru.yandex.yt.ytclient.ytree.YTreeStringNode;
import ru.yandex.yt.ytclient.ytree.YTreeUint64Node;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@RunWith(Parameterized.class)
public class YsonTextEncoderTest {
    @Parameterized.Parameter
    public boolean withAttributes;

    @Parameterized.Parameters(name = "{index}: {0}")
    public static Object[] params() {
        return new Object[]{false, true};
    }

    private Map<String, YTreeNode> attributes() {
        Map<String, YTreeNode> attributes = new LinkedHashMap<>();
        if (withAttributes) {
            attributes.put("a", new YTreeInt64Node(42L));
            attributes.put("b", new YTreeEntityNode());
        }
        return attributes;
    }

    private String prefix() {
        return withAttributes ? "<\"a\"=42;\"b\"=#>" : "";
    }

    @Test
    public void simpleNull() {
        assertThat(YsonTextEncoder.encode(null), is("#"));
    }

    @Test
    public void entityNode() {
        assertThat(YsonTextEncoder.encode(new YTreeEntityNode(attributes())), is(prefix() + "#"));
    }

    @Test
    public void stringNode() {
        assertThat(YsonTextEncoder.encode(new YTreeStringNode("hello", attributes())), is(prefix() + "\"hello\""));
    }

    @Test
    public void unsignedNode() {
        assertThat(YsonTextEncoder.encode(new YTreeUint64Node(42L, attributes())), is(prefix() + "42u"));
    }

    @Test
    public void doubleNode() {
        assertThat(YsonTextEncoder.encode(new YTreeDoubleNode(1.5, attributes())), is(prefix() + "1.5"));
    }

    @Test
    public void falseNode() {
        assertThat(YsonTextEncoder.encode(new YTreeBooleanNode(false, attributes())), is(prefix() + "%false"));
    }

    @Test
    public void trueNode() {
        assertThat(YsonTextEncoder.encode(new YTreeBooleanNode(true, attributes())), is(prefix() + "%true"));
    }

    @Test
    public void listNode() {
        List<YTreeNode> list = Arrays.asList(
                new YTreeStringNode("c"),
                new YTreeInt64Node(51L),
                new YTreeBooleanNode(true));
        assertThat(YsonTextEncoder.encode(new YTreeListNode(list, attributes())), is(prefix() + "[\"c\";51;%true]"));
    }

    @Test
    public void mapNode() {
        Map<String, YTreeNode> map = new LinkedHashMap<>();
        map.put("c", new YTreeInt64Node(51L));
        map.put("d\"e", new YTreeBooleanNode(false));
        assertThat(YsonTextEncoder.encode(new YTreeMapNode(map, attributes())),
                is(prefix() + "{\"c\"=51;\"d\\\"e\"=%false}"));
    }
}
