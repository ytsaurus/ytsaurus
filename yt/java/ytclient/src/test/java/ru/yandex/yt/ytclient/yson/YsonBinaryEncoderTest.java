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
public class YsonBinaryEncoderTest {
    @Parameterized.Parameter
    public boolean withAttributes;

    @Parameterized.Parameters(name = "{index}: {0}")
    public static Object[] params() {
        return new Object[]{false, true};
    }

    public Map<String, YTreeNode> attributes() {
        Map<String, YTreeNode> attributes = new LinkedHashMap<>();
        if (withAttributes) {
            attributes.put("a", new YTreeInt64Node(42L));
            attributes.put("b", new YTreeEntityNode());
        }
        return attributes;
    }

    public byte[] prefix() {
        if (withAttributes) {
            return new byte[]{'<', 1, 2, 'a', '=', 2, 84, ';', 1, 2, 'b', '=', '#', '>'};
        } else {
            return new byte[0];
        }
    }

    public static byte[] combine(byte[]... parts) {
        int length = 0;
        for (byte[] part : parts) {
            length += part.length;
        }
        byte[] combined = new byte[length];
        length = 0;
        for (byte[] part : parts) {
            System.arraycopy(part, 0, combined, length, part.length);
            length += part.length;
        }
        return combined;
    }

    @Test
    public void simpleNull() {
        assertThat(YsonBinaryEncoder.encode(null), is(new byte[]{'#'}));
    }

    @Test
    public void entityNode() {
        assertThat(YsonBinaryEncoder.encode(new YTreeEntityNode(attributes())), is(combine(prefix(), new byte[]{'#'})));
    }

    @Test
    public void stringNode() {
        assertThat(YsonBinaryEncoder.encode(new YTreeStringNode("hello", attributes())),
                is(combine(prefix(), new byte[]{1, 10, 'h', 'e', 'l', 'l', 'o'})));
    }

    @Test
    public void unsignedNode() {
        assertThat(YsonBinaryEncoder.encode(new YTreeUint64Node(42L, attributes())),
                is(combine(prefix(), new byte[]{6, 42})));
    }

    @Test
    public void doubleNode() {
        assertThat(YsonBinaryEncoder.encode(new YTreeDoubleNode(1.5, attributes())),
                is(combine(prefix(), new byte[]{3, 0, 0, 0, 0, 0, 0, -8, 63})));
    }

    @Test
    public void falseNode() {
        assertThat(YsonBinaryEncoder.encode(new YTreeBooleanNode(false, attributes())),
                is(combine(prefix(), new byte[]{4})));
    }

    @Test
    public void trueNode() {
        assertThat(YsonBinaryEncoder.encode(new YTreeBooleanNode(true, attributes())),
                is(combine(prefix(), new byte[]{5})));
    }

    @Test
    public void listNode() {
        List<YTreeNode> list = Arrays.asList(
                new YTreeStringNode("c"),
                new YTreeInt64Node(51L),
                new YTreeBooleanNode(true));
        assertThat(YsonBinaryEncoder.encode(new YTreeListNode(list, attributes())),
                is(combine(prefix(), new byte[]{'[', 1, 2, 'c', ';', 2, 102, ';', 5, ']'})));
    }

    @Test
    public void mapNode() {
        Map<String, YTreeNode> map = new LinkedHashMap<>();
        map.put("c", new YTreeInt64Node(51L));
        map.put("d\"e", new YTreeBooleanNode(false));
        assertThat(YsonBinaryEncoder.encode(new YTreeMapNode(map, attributes())),
                is(combine(prefix(), new byte[]{'{', 1, 2, 'c', '=', 2, 102, ';', 1, 6, 'd', '"', 'e', '=', 4, '}'})));
    }
}
