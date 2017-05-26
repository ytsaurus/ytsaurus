package ru.yandex.yt.ytclient.yson;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.protobuf.CodedInputStream;
import org.junit.Test;

import ru.yandex.yt.ytclient.ytree.YTreeBooleanNode;
import ru.yandex.yt.ytclient.ytree.YTreeBuilder;
import ru.yandex.yt.ytclient.ytree.YTreeDoubleNode;
import ru.yandex.yt.ytclient.ytree.YTreeEntityNode;
import ru.yandex.yt.ytclient.ytree.YTreeInt64Node;
import ru.yandex.yt.ytclient.ytree.YTreeListNode;
import ru.yandex.yt.ytclient.ytree.YTreeMapNode;
import ru.yandex.yt.ytclient.ytree.YTreeNode;
import ru.yandex.yt.ytclient.ytree.YTreeStringNode;
import ru.yandex.yt.ytclient.ytree.YTreeUint64Node;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;

public class YsonBinaryDecoderTest {
    private static CodedInputStream cis(String text) {
        return CodedInputStream.newInstance(text.getBytes(StandardCharsets.UTF_8));
    }

    private static YTreeNode parseNode(String text) throws IOException {
        return YsonBinaryDecoder.parseNode(cis(text));
    }

    private static YTreeNode parseNode(byte[] bytes) throws IOException {
        return YsonBinaryDecoder.parseNode(CodedInputStream.newInstance(bytes));
    }

    private static YTreeMapNode parseMapFragment(String text) throws IOException {
        return YsonBinaryDecoder.parseMapFragment(cis(text));
    }

    private static YTreeListNode parseListFragment(String text) throws IOException {
        return YsonBinaryDecoder.parseListFragment(cis(text));
    }

    @Test
    public void unquotedString() throws IOException {
        assertThat(parseNode("hello"), is(new YTreeStringNode("hello")));
    }

    @Test
    public void quotedString() throws IOException {
        assertThat(parseNode("\"he\\\"llo\""), is(new YTreeStringNode("he\"llo")));
    }

    @Test
    public void binaryString() throws IOException {
        assertThat(parseNode(new byte[]{1, 10, 'h', 'e', 'l', 'l', 'o'}), is(new YTreeStringNode("hello")));
    }

    @Test
    public void textSigned() throws IOException {
        assertThat(parseNode("-42"), is(new YTreeInt64Node(-42L)));
    }

    @Test
    public void textUnsigned() throws IOException {
        assertThat(parseNode("42u"), is(new YTreeUint64Node(42L)));
    }

    @Test
    public void binarySigned() throws IOException {
        assertThat(parseNode(new byte[]{2, 84}), is(new YTreeInt64Node(42L)));
        assertThat(parseNode(new byte[]{2, 83}), is(new YTreeInt64Node(-42L)));
    }

    @Test
    public void binaryUnsigned() throws IOException {
        assertThat(parseNode(new byte[]{6, 42}), is(new YTreeUint64Node(42L)));
    }

    @Test
    public void textDouble() throws IOException {
        assertThat(parseNode("1.5"), is(new YTreeDoubleNode(1.5)));
    }

    @Test
    public void binaryDouble() throws IOException {
        assertThat(parseNode(new byte[]{3, 0, 0, 0, 0, 0, 0, -8, 63}), is(new YTreeDoubleNode(1.5)));
    }

    @Test
    public void textBoolean() throws IOException {
        assertThat(parseNode("%false"), is(YTreeBooleanNode.FALSE));
        assertThat(parseNode("%true"), is(YTreeBooleanNode.TRUE));
    }

    @Test
    public void binaryBoolean() throws IOException {
        assertThat(parseNode(new byte[]{4}), is(YTreeBooleanNode.FALSE));
        assertThat(parseNode(new byte[]{5}), is(YTreeBooleanNode.TRUE));
    }

    @Test
    public void entity() throws IOException {
        assertThat(parseNode("#"), is(YTreeEntityNode.INSTANCE));
    }

    @Test
    public void entityWithAttributes() throws IOException {
        Map<String, YTreeNode> attributes = new HashMap<>();
        attributes.put("a", new YTreeInt64Node(42L));
        attributes.put("b", YTreeBooleanNode.FALSE);
        assertThat(parseNode("<a=42;b=%false>#"), is(new YTreeEntityNode(attributes)));
    }

    @Test
    public void textMap() throws IOException {
        assertThat(parseNode("{a=1;b=2}"),
                is(new YTreeBuilder().beginMap().key("a").value(1).key("b").value(2).buildMap()));
    }

    @Test
    public void binaryMap() throws IOException {
        assertThat(parseNode(new byte[]{'{', 1, 2, 'a', '=', 2, 2, ';', 1, 2, 'b', '=', 2, 4, '}'}),
                is(new YTreeBuilder().beginMap().key("a").value(1).key("b").value(2).buildMap()));
    }

    @Test(expected = YsonFormatException.class)
    public void nonStringMapKey() throws IOException {
        parseNode(new byte[]{'{', 2, 2, '=', 2, 4, '}'});
    }

    @Test(expected = YsonFormatException.class)
    public void missingKeyValueSeparator() throws IOException {
        parseNode("{a b}");
    }

    @Test(expected = YsonFormatException.class)
    public void missingMapItemSeparator() throws IOException {
        parseNode("{a=1 b=2}");
    }

    @Test
    public void listNode() throws IOException {
        assertThat(parseNode("[1;2;3]"), is(new YTreeBuilder().beginList().value(1).value(2).value(3).buildList()));
    }

    @Test(expected = YsonFormatException.class)
    public void missingListItemSeparator() throws IOException {
        parseNode("[1 2 3]");
    }

    @Test
    public void whitespaceSkip() throws IOException {
        Map<String, YTreeNode> attributes = new HashMap<>();
        attributes.put("a", new YTreeInt64Node(42L));
        attributes.put("b", YTreeBooleanNode.FALSE);
        List<YTreeNode> list = Arrays.asList(
                new YTreeInt64Node(1L),
                new YTreeInt64Node(2L),
                new YTreeInt64Node(3L));
        assertThat(parseNode("<  a  =  42;  \"b\"  =  %false  >  [1  ;  2  ;  3  ;  ]  "),
                is(new YTreeListNode(list, attributes)));
    }

    @Test
    public void hexEscapes() throws IOException {
        List<YTreeNode> parsed = Arrays.asList(
                parseNode("\"\\x3F\""),
                parseNode("\"\\x3\""),
                parseNode("\"\\x\""));
        assertThat(parsed, contains(
                new YTreeStringNode("?"),
                new YTreeStringNode("\u0003"),
                new YTreeStringNode("x")));
    }

    @Test
    public void octEscapes() throws IOException {
        List<YTreeNode> parsed = Arrays.asList(
                parseNode("\"\\000\""),
                parseNode("\"\\00\""),
                parseNode("\"\\0\""),
                parseNode("\"\\400\""));
        assertThat(parsed, contains(
                new YTreeStringNode("\0"),
                new YTreeStringNode("\0"),
                new YTreeStringNode("\0"),
                new YTreeStringNode(" 0")));
    }

    @Test
    public void mapFragment() throws IOException {
        assertThat(parseMapFragment("a=1;b=2"),
                is(new YTreeBuilder().beginMap().key("a").value(1).key("b").value(2).buildMap()));
    }

    @Test
    public void listFragment() throws IOException {
        assertThat(parseListFragment("1;2;3"),
                is(new YTreeBuilder().beginList().value(1).value(2).value(3).buildList()));
    }

    @Test(expected = YsonUnexpectedEOF.class)
    public void truncatedMap() throws IOException {
        parseNode("{a=1;b=2");
    }

    @Test(expected = YsonUnexpectedEOF.class)
    public void truncatedList() throws IOException {
        parseNode("[1;2;3");
    }

    @Test(expected = YsonUnexpectedEOF.class)
    public void truncatedAttributes() throws IOException {
        parseNode("<a=1;b=2");
    }

    @Test(expected = YsonUnexpectedEOF.class)
    public void truncatedTrue() throws IOException {
        parseNode("%tru");
    }

    @Test(expected = YsonUnexpectedEOF.class)
    public void truncatedFalse() throws IOException {
        parseNode("%fals");
    }

    @Test(expected = YsonUnexpectedEOF.class)
    public void truncatedBoolean() throws IOException {
        parseNode("%");
    }

    @Test(expected = YsonUnexpectedEOF.class)
    public void emptyNode() throws IOException {
        parseNode("");
    }

    @Test
    public void emptyMap() throws IOException {
        assertThat(parseMapFragment(""), is(YTreeMapNode.EMPTY));
    }

    @Test
    public void emptyList() throws IOException {
        assertThat(parseListFragment(""), is(YTreeListNode.EMPTY));
    }

    @Test(expected = YsonFormatException.class)
    public void extraTokens() throws IOException {
        parseNode("42   ,");
    }
}
