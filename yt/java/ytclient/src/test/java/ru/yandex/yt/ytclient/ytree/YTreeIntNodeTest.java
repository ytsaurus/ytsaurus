package ru.yandex.yt.ytclient.ytree;

import org.hamcrest.Matchers;
import org.junit.Test;

import ru.yandex.yt.ytclient.yson.YsonFormatException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class YTreeIntNodeTest {
    @Test
    public void zeroSignedUnsignedEqual() {
        YTreeInt64Node a = new YTreeInt64Node(0L);
        YTreeUint64Node b = new YTreeUint64Node(0L);
        assertThat(a, is(b));
        assertThat(b, is(a));
        assertThat(a.hashCode(), is(b.hashCode()));
        assertThat(a.longValue(), is(0L));
        assertThat(b.longValue(), is(0L));
        assertThat(a.isSigned(), is(true));
        assertThat(b.isSigned(), is(false));
    }

    @Test
    public void positiveSignedUnsignedEqual() {
        YTreeInt64Node a = new YTreeInt64Node(42L);
        YTreeUint64Node b = new YTreeUint64Node(42L);
        assertThat(a, is(b));
        assertThat(b, is(a));
        assertThat(a.hashCode(), is(b.hashCode()));
        assertThat(a.longValue(), is(42L));
        assertThat(b.longValue(), is(42L));
        assertThat(a.isSigned(), is(true));
        assertThat(b.isSigned(), is(false));
    }

    @Test
    public void negativeSignedUnsignedNotEqual() {
        YTreeInt64Node a = new YTreeInt64Node(-42L);
        YTreeUint64Node b = new YTreeUint64Node(-42L);
        assertThat(a, is(Matchers.not(b)));
        assertThat(b, is(not(a)));
        assertThat(a.longValue(), is(-42L));
        assertThat(b.longValue(), is(-42L));
        assertThat(a.isSigned(), is(true));
        assertThat(b.isSigned(), is(false));
    }

    @Test
    public void signedIntExtension() {
        YTreeInt64Node node = new YTreeInt64Node(-42);
        assertThat(node.longValue(), is(-42L));
    }

    @Test
    public void unsignedIntExtension() {
        YTreeUint64Node node = new YTreeUint64Node(-42);
        assertThat(node.longValue(), is(4294967254L));
    }

    @Test
    public void signedText() {
        assertThat(new YTreeInt64Node(-42L).toString(), is("-42"));
    }

    @Test
    public void signedBinary() {
        assertThat(new YTreeInt64Node(-42L).toBinary(), is(new byte[]{2, 83}));
    }

    @Test
    public void unsignedText() {
        assertThat(new YTreeUint64Node(-42L).toString(), is("18446744073709551574u"));
    }

    @Test
    public void unsignedBinary() {
        assertThat(new YTreeUint64Node(42L).toBinary(), is(new byte[]{6, 42}));
    }

    @Test
    public void parseSignedText() {
        assertThat(YTreeNode.parseString("-42"), is(new YTreeInt64Node(-42L)));
    }

    @Test
    public void parseSignedBinary() {
        assertThat(YTreeNode.parseByteArray(new byte[]{2, 83}), is(new YTreeInt64Node(-42L)));
    }

    @Test
    public void parseUnsignedText() {
        assertThat(YTreeNode.parseString("18446744073709551574u"), is(new YTreeUint64Node(-42L)));
    }

    @Test
    public void parseUnsignedBinary() {
        assertThat(YTreeNode.parseByteArray(new byte[]{6, 83}), is(new YTreeUint64Node(83L)));
    }

    @Test(expected = YsonFormatException.class)
    public void parseInvalidSigned() {
        YTreeNode.parseString("-9223372036854775809");
    }

    @Test(expected = YsonFormatException.class)
    public void parseInvalidUnsigned() {
        YTreeNode.parseString("18446744073709551616u");
    }
}
