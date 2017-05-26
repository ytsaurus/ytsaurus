package ru.yandex.yt.ytclient.ytree;

import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class YTreeStringNodeTest {
    @Test
    public void normalStringValue() {
        YTreeStringNode a = new YTreeStringNode("hello");
        assertThat(a.stringValue(), is("hello"));
    }

    @Test
    public void sameNormalStringsEqual() {
        YTreeStringNode a = new YTreeStringNode("hello");
        YTreeStringNode b = new YTreeStringNode("hello");
        assertThat(a, is(b));
        assertThat(b, is(a));
        assertThat(a.hashCode(), is(b.hashCode()));
    }

    @Test
    public void differentNormalStringsNotEqual() {
        YTreeStringNode a = new YTreeStringNode("hello");
        YTreeStringNode b = new YTreeStringNode("world");
        assertThat(a, is(not(b)));
        assertThat(b, is(not(a)));
    }

    @Test
    public void binaryStringValue() {
        YTreeStringNode a = new YTreeStringNode(new byte[]{(byte) 0x80, (byte) 0x81, (byte) 0x82});
        assertThat(a.stringValue(), is("\ufffd\ufffd\ufffd"));
        assertThat(a.bytesValue(), is(new byte[]{(byte) 0x80, (byte) 0x81, (byte) 0x82}));
    }

    @Test
    public void sameBinaryStringsEqual() {
        YTreeStringNode a = new YTreeStringNode(new byte[]{(byte) 0x80, (byte) 0x81, (byte) 0x82});
        YTreeStringNode b = new YTreeStringNode(new byte[]{(byte) 0x80, (byte) 0x81, (byte) 0x82});
        assertThat(a, is(b));
        assertThat(b, is(a));
        assertThat(a.hashCode(), is(b.hashCode()));
    }

    @Test
    public void differentBinaryStringsNotEqual() {
        YTreeStringNode a = new YTreeStringNode(new byte[]{(byte) 0x80, (byte) 0x81, (byte) 0x82});
        YTreeStringNode b = new YTreeStringNode(new byte[]{(byte) 0x80, (byte) 0x81, (byte) 0x83});
        assertThat(a, is(not(b)));
        assertThat(b, is(not(a)));
        // На всякий случай проверяем, что текст действительно одинаковый
        assertThat(a.stringValue(), is(b.stringValue()));
    }

    @Test
    public void minimalOctalEscaping() {
        assertThat(new YTreeStringNode("hello\0\1\2\3\4\5\6\7world").toString(),
                is("\"hello\\0\\1\\2\\3\\4\\5\\6\\7world\""));
    }

    @Test
    public void minimalOctalParsing() {
        assertThat(YTreeNode.parseString("\"hello\\0\\1\\2\\3\\4\\5\\6\\7world\""),
                is(new YTreeStringNode("hello\0\1\2\3\4\5\6\7world")));
    }

    @Test
    public void shortOctalEscaping() {
        assertThat(new YTreeStringNode("hello\u000099world").toString(),
                is("\"hello\\099world\""));
    }

    @Test
    public void longOctalEscaping() {
        assertThat(new YTreeStringNode("hello\u000042world").toString(),
                is("\"hello\\00042world\""));
    }

    @Test
    public void normalHexEscaping() {
        assertThat(new YTreeStringNode("hello\u001fworld").toString(),
                is("\"hello\\x1fworld\""));
    }

    @Test
    public void disallowedHexEscaping() {
        assertThat(new YTreeStringNode("hello\u001ffabulous").toString(),
                is("\"hello\\037fabulous\""));
    }

    @Test
    public void russianEscaping() {
        assertThat(new YTreeStringNode("проверка").toString(),
                is("\"\\xd0\\xbf\\xd1\\x80\\xd0\\xbe\\xd0\\xb2\\xd0\\xb5\\xd1\\x80\\xd0\\xba\\xd0\\xb0\""));
    }

    @Test
    public void russianEscapedParsing() {
        assertThat(YTreeNode
                        .parseString("\"\\xd0\\xbf\\xd1\\x80\\xd0\\xbe\\xd0\\xb2\\xd0\\xb5\\xd1\\x80\\xd0\\xba\\xd0\\xb0\""),
                is(new YTreeStringNode("проверка")));
    }

    @Test
    public void russianUnescapedParsing() {
        // Проверяем, что наш парсер нормально парсит голый UTF-8
        assertThat(YTreeNode.parseString("\"проверка\""), is(new YTreeStringNode("проверка")));
    }

    @Test
    public void unquotedParsing() {
        assertThat(YTreeNode.parseString("hello"), is(new YTreeStringNode("hello")));
    }

    @Test
    public void binaryEncoding() {
        assertThat(new YTreeStringNode("hello").toBinary(), is(new byte[]{1, 10, 'h', 'e', 'l', 'l', 'o'}));
    }

    @Test
    public void binaryParsing() {
        assertThat(YTreeNode.parseByteArray(new byte[]{1, 10, 'h', 'e', 'l', 'l', 'o'}),
                is(new YTreeStringNode("hello")));
    }
}
