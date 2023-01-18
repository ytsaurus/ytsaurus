package tech.ytsaurus.yson;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.StringReader;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class YsonParserTest {
    final String testType;

    public YsonParserTest(String testType) {
        this.testType = testType;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<String> testTypes() {
        return List.of("stream", "bytes");
    }

    // Simple function that decodes escape sequences like \xFF
    static byte[] unescapeBytes(String data) {
        List<Byte> byteList = new ArrayList<>();
        StringReader dataInput = new StringReader(data);
        char[] hexBuf = new char[2];

        try {
            while (true) {
                int c = dataInput.read();
                if (c < 0) {
                    break;
                }
                if (c != '\\') {
                    byteList.add((byte) c);
                } else {
                    c = dataInput.read();
                    switch (c) {
                        case -1:
                            throw new RuntimeException("unterminated escape sequence");
                        case '\\':
                            byteList.add((byte) '\\');
                            break;
                        case 'x':
                            if (dataInput.read(hexBuf) < 2) {
                                throw new RuntimeException("unterminated escape sequence");
                            }
                            byte b = (byte) Integer.parseInt(new String(hexBuf), 16);
                            byteList.add(b);
                            break;
                        default:
                            throw new RuntimeException("bad escape sequence: \\" + c);
                    }
                }

            }
        } catch (IOException exception) {
            throw new UncheckedIOException(exception);
        }

        byte[] result = new byte[byteList.size()];
        for (int i = 0; i < byteList.size(); ++i) {
            result[i] = byteList.get(i);
        }
        return result;
    }

    @Test
    public void testUnescapeBytes() {
        assertArrayEquals(unescapeBytes("ab\\x5Fcd\\xf5"), new byte[]{'a', 'b', 0x5f, 'c', 'd', -11});
    }

    public YsonParser createParser(byte[] bytes) {
        if (testType.equals("stream")) {
            return new YsonParser(new ByteArrayInputStream(bytes));
        } else if (testType.equals("bytes")) {
            return new YsonParser(bytes);
        } else {
            throw new IllegalStateException();
        }
    }

    public String canonizeNode(String escapedBytes) {
        final byte[] binary = unescapeBytes(escapedBytes);

        StringBuilder result = new StringBuilder();
        YsonParser parser = createParser(binary);

        try (YsonTextWriter textWriter = new YsonTextWriter(result)) {
            parser.parseNode(textWriter);
        }
        return result.toString();
    }

    public String canonizeListFragment(String escapedBytes) {
        final byte[] binary = unescapeBytes(escapedBytes);

        StringBuilder result = new StringBuilder();
        YsonParser parser = createParser(binary);
        try (ClosableYsonConsumer textWriter = new FragmentYsonTextWriter(result)) {
            parser.parseListFragment(textWriter);
        }
        return result.toString();
    }

    @Test
    public void testEntity() {
        assertEquals(canonizeNode("#"), "#");
    }

    @Test
    public void testBinaryString() {
        YsonError e;

        assertEquals(canonizeNode("\\x01\\x00"), "\"\"");
        assertEquals(canonizeNode("\\x01\\x06foo"), "\"foo\"");

        e = assertThrows(YsonError.class, () -> canonizeNode("\\x01\\x03foo"));
        assertTrue(e.getMessage().contains("Yson string length is negative"));

        e = assertThrows(YsonError.class, () -> canonizeNode("\\x01\\x84\\x85\\x86\\x87\\x88\\x89\\x02foo"));
        assertTrue(e.getMessage().contains("Yson string length exceeds limit"));

        e = assertThrows(YsonError.class, () -> canonizeNode("\\x01\\x08foo"));
        assertTrue(e.getMessage().contains("Unexpected end of stream"));

        e = assertThrows(YsonError.class, () -> canonizeNode("\\x01\\x82"));
        assertTrue(e.getMessage().contains("Unexpected end of stream"));
    }

    @Test
    public void testBinaryInt64() {
        assertEquals(canonizeNode("\\x02\\x00"), "0");
        assertEquals(canonizeNode("\\x02\\x01"), "-1");
        assertEquals(canonizeNode("\\x02\\x02"), "1");
        assertEquals(canonizeNode("\\x02\\x06"), "3");
        assertEquals(canonizeNode("\\x02\\x84\\x85\\x86\\x87\\x88\\x89\\x02"), "4553746465090");
        assertEquals(
                canonizeNode("\\x02\\x81\\xd5\\xf4\\xd9\\xea\\xa4\\xbd\\xd3\\x95\\x01"),
                "-5391787952107853121");

        assertThrows(YsonError.class,
                () -> canonizeNode("\\x02\\x84\\x85\\x86\\x87\\x88\\x89\\x90\\x91\\x92\\x93\\x72"));
        assertThrows(YsonError.class,
                () -> canonizeNode("\\x02\\x84\\x85\\x86\\x87\\x88\\x89\\x90\\x91\\x92\\x93"));

    }

    @Test
    public void testBinaryUint64() {
        assertEquals(canonizeNode("\\x06\\x00"), "0u");
        assertEquals(canonizeNode("\\x06\\x01"), "1u");
        assertEquals(canonizeNode("\\x06\\x02"), "2u");
        assertEquals(canonizeNode("\\x06\\x06"), "6u");
        assertEquals(canonizeNode("\\x06\\x84\\x85\\x86\\x87\\x88\\x89\\x02"), "9107492930180u");

        assertThrows(YsonError.class,
                () -> canonizeNode("\\x06\\x84\\x85\\x86\\x87\\x88\\x89\\x90\\x91\\x92\\x93\\x72"));
        assertThrows(YsonError.class,
                () -> canonizeNode("\\x06\\x84\\x85\\x86\\x87\\x88\\x89\\x90\\x91\\x92\\x93"));
    }

    @Test
    public void testBinaryDouble() {
        assertEquals(canonizeNode("\\x03\\x00\\x00\\x00\\x00\\x00\\x00\\x0A\\x40"), "3.25");
        assertEquals(canonizeNode("\\x03\\x00\\x00\\x00\\x00\\x00\\x00\\xc0\\xbf"), "-0.125");
        assertEquals(canonizeNode("\\x03\\x00\\x00\\x00\\x00\\x00\\x00\\xf8\\x7f"), "%nan");
    }

    @Test
    public void testBinaryBoolean() {
        assertEquals(canonizeNode("\\x04"), "%false");
        assertEquals(canonizeNode("\\x05"), "%true");
    }

    @Test
    public void testUnquotedString() {
        assertEquals(canonizeNode("foo"), "\"foo\"");
        assertEquals(canonizeNode("bar25"), "\"bar25\"");
        assertEquals(canonizeNode("_"), "\"_\"");
        assertEquals(canonizeNode("foo.bar"), "\"foo.bar\"");
    }

    @Test
    public void testQuotedString() {
        assertEquals(canonizeNode("\"foo\""), "\"foo\"");
        assertEquals(canonizeNode("\"25\""), "\"25\"");
        assertEquals(canonizeNode("\"foo\""), "\"foo\"");
        assertEquals(canonizeNode("\"\\\\000\""), "\"\\x00\"");
        assertEquals(canonizeNode("\"\\\\102\""), "\"B\"");
        assertEquals(canonizeNode("\"\\\\377\""), "\"\\xff\"");
        assertEquals(canonizeNode("\"\\\\x42\""), "\"B\"");

        assertThrows(YsonError.class,
                () -> canonizeNode("\"\\\\400\""));
        assertThrows(YsonError.class,
                () -> canonizeNode("\"\\\\x8\""));
    }

    @Test
    public void testTextBoolean() {
        assertEquals(canonizeNode("%true"), "%true");
        assertEquals(canonizeNode("%false"), "%false");
    }

    @Test
    public void testTextInt64() {
        assertEquals(canonizeNode("-100500"), "-100500");
        assertEquals(canonizeNode("100500"), "100500");
        assertEquals(canonizeNode("+100500"), "100500");
        assertEquals(canonizeNode("+0"), "0");
        assertEquals(canonizeNode("-0"), "0");
        assertThrows(YsonError.class,
                () -> canonizeNode("10050000000000000000000000000000000000000000000000"));
    }

    @Test
    public void testTextUint64() {
        assertEquals(canonizeNode("100500u"), "100500u");
        assertEquals(canonizeNode("+100500u"), "100500u");
        assertEquals(canonizeNode("+0u"), "0u");
        assertThrows(YsonError.class,
                () -> canonizeNode("-100500u"));
        assertThrows(YsonError.class,
                () -> canonizeNode("10050000000000000000000000000000000000000000000000u"));
    }

    @Test
    public void testTextDouble() {
        assertEquals(canonizeNode("3125e-3"), "3.125");
        assertEquals(canonizeNode(".125"), "0.125");
        assertEquals(canonizeNode(" %nan "), "%nan");
        assertEquals(canonizeNode("%inf"), "%+inf");
        assertEquals(canonizeNode("%+inf"), "%+inf");
        assertEquals(canonizeNode("%-inf"), "%-inf");

        assertThrows(YsonError.class,
                () -> canonizeNode("%+nan"));
        assertThrows(YsonError.class,
                () -> canonizeNode("%-nan"));

        assertThrows(YsonError.class,
                () -> canonizeNode("%+nany"));
        assertThrows(YsonError.class,
                () -> canonizeNode("+n"));
        assertThrows(YsonError.class,
                () -> canonizeNode("+inf"));
        assertThrows(YsonError.class,
                () -> canonizeNode("%infinity"));
    }

    @Test
    public void testList() {
        assertEquals(canonizeNode("[]"), "[]");
        assertEquals(canonizeNode("[ ]"), "[]");
        assertEquals(canonizeNode("[1;2;3]"), "[1;2;3;]");
        assertEquals(canonizeNode("[ 1 ; 2 ; 3 ;]"), "[1;2;3;]");

        assertThrows(
                YsonError.class,
                () -> canonizeNode("[;]"));
        assertThrows(YsonError.class,
                () -> canonizeNode("[;;]"));
        assertThrows(YsonError.class,
                () -> canonizeNode("[1;; 2]"));

        assertThrows(YsonError.class,
                () -> canonizeNode("[1 2]"));
    }

    @Test
    public void testMap() {
        assertEquals(canonizeNode("{}"), "{}");
        assertEquals(canonizeNode("{ }"), "{}");
        assertEquals(canonizeNode("{foo=bar}"), "{\"foo\"=\"bar\";}");
        assertEquals(canonizeNode("{foo=bar;}"), "{\"foo\"=\"bar\";}");
        assertEquals(canonizeNode("{foo=bar;bar=baz}"), "{\"foo\"=\"bar\";\"bar\"=\"baz\";}");
        assertEquals(canonizeNode(" { foo = bar ; bar = 42 ; } "), "{\"foo\"=\"bar\";\"bar\"=42;}");

        assertThrows(
                YsonError.class,
                () -> canonizeNode("{;}"));
        assertThrows(
                YsonError.class,
                () -> canonizeNode("{foo}"));
        assertThrows(
                YsonError.class,
                () -> canonizeNode("{foo=}"));
        assertThrows(
                YsonError.class,
                () -> canonizeNode("{1=foo}"));
    }

    @Test
    public void testAttributes() {
        assertEquals(canonizeNode("<>42"), "<>42");
        assertEquals(canonizeNode("<foo=bar>42"), "<\"foo\"=\"bar\";>42");
        assertEquals(canonizeNode("<foo=bar;>42"), "<\"foo\"=\"bar\";>42");
        assertEquals(canonizeNode("<foo=<baz=32>bar;>42"), "<\"foo\"=<\"baz\"=32;>\"bar\";>42");
        assertEquals(canonizeNode(" < foo = < baz = 32 > bar ; > 42"), "<\"foo\"=<\"baz\"=32;>\"bar\";>42");

        assertThrows(
                YsonError.class,
                () -> canonizeNode("<;>42"));
        assertThrows(
                YsonError.class,
                () -> canonizeNode("<42=bar>42"));
        assertThrows(
                YsonError.class,
                () -> canonizeNode("<foo=>82"));
        assertThrows(
                YsonError.class,
                () -> canonizeNode("<1=foo> 45"));
    }

    @Test
    public void testComplexNodes() {
        assertEquals(canonizeNode(
                "[123;\\x02\\x82\\x06;{\\x01\\x02a=\\x02\\x84\\x34;\\x01\\x06foo=<bar=\"baz \">\\x05}]"
        ), "[123;385;{\"a\"=3330;\"foo\"=<\"bar\"=\"baz \";>%true;};]");
    }

    @Test
    public void testBufferBoundaries() {
        final String textData = "[" +
                "123;" +
                "\\x02\\x82\\x06;" +
                "{\\x01\\x02a=\\x02\\x84\\x34;\\x01\\x06foo=<bar=\"baz \">\\x05};" +
                "\\x06\\x80\\x80\\x84\\x85\\x86\\x87\\x88\\x89\\x02;" +
                "\\x03\\x00\\x00\\x00\\x00\\x00\\x00\\xc0\\xbf" +
                "]";
        final String expectedCanonized = "[" +
                "123;" +
                "385;" +
                "{\"a\"=3330;\"foo\"=<\"bar\"=\"baz \";>%true;};" +
                "149217164168069120u;" +
                "-0.125;" +
                "]";
        byte[] binaryData = unescapeBytes(textData);
        for (int bufferSize = 1; bufferSize != binaryData.length; ++bufferSize) {
            StringBuilder result = new StringBuilder();
            YsonParser parser = new YsonParser(new ByteArrayInputStream(binaryData), bufferSize);
            try (YsonTextWriter textWriter = new YsonTextWriter(result)) {
                parser.parseNode(textWriter);
            }
            assertEquals(result.toString(), expectedCanonized);
        }
    }

    @Test
    public void testListFragment() {
        assertEquals(canonizeListFragment(""), "");
        assertEquals(canonizeListFragment(" "), "");
        assertEquals(canonizeListFragment("1;2;3"), "1;2;3");
        assertEquals(canonizeListFragment("1 ; 2 ; 3"), "1;2;3");

        assertThrows(
                YsonError.class,
                () -> canonizeListFragment(";"));
        assertThrows(YsonError.class,
                () -> canonizeListFragment(";;"));
        assertThrows(YsonError.class,
                () -> canonizeListFragment("1;; 2"));

        assertThrows(YsonError.class,
                () -> canonizeListFragment("1 2"));
    }

    @Test
    public void testListFragmentItem() {
        final String textData = "123;\\x02\\x82\\x06;{\\x01\\x02a=\\x02\\x84\\x34;\\x01\\x06foo=<bar=\"baz \">\\x05};" +
                "\\x06\\x80\\x80\\x84\\x85\\x86\\x87\\x88\\x89\\x02";
        final byte[] binaryData = unescapeBytes(textData);

        YsonParser parser = createParser(binaryData);

        Supplier<String> nextItem = () -> {
            StringBuilder result = new StringBuilder();
            boolean parsed;
            try (YsonTextWriter writer = new YsonTextWriter(result)) {
                parsed = parser.parseListFragmentItem(writer);
            }

            if (parsed) {
                return result.toString();
            } else {
                return null;
            }
        };

        assertEquals(nextItem.get(), "123");
        assertEquals(nextItem.get(), "385");
        assertEquals(nextItem.get(), "{\"a\"=3330;\"foo\"=<\"bar\"=\"baz \";>%true;}");
        assertEquals(nextItem.get(), "149217164168069120u");
        assertNull(nextItem.get());

    }
}

class FragmentYsonTextWriter implements ClosableYsonConsumer {
    private final StringBuilder stringBuilder;
    @Nullable private ClosableYsonConsumer underlying;
    private int depth = 0;
    private boolean first = true;

    FragmentYsonTextWriter(StringBuilder sb) {
        stringBuilder = sb;
    }

    @Override
    public void onEntity() {
        assert underlying != null;
        underlying.onEntity();
    }

    @Override
    public void onInteger(long value) {
        assert underlying != null;
        underlying.onInteger(value);
    }

    @Override
    public void onUnsignedInteger(long value) {
        assert underlying != null;
        underlying.onUnsignedInteger(value);
    }

    @Override
    public void onBoolean(boolean value) {
        assert underlying != null;
        underlying.onBoolean(value);
    }

    @Override
    public void onDouble(double value) {
        assert underlying != null;
        underlying.onDouble(value);
    }

    @Override
    public void onString(@Nonnull byte[] value, int offset, int length) {
        assert underlying != null;
        underlying.onString(value, offset, length);
    }

    @Override
    public void onBeginList() {
        assert underlying != null;
        depth++;
        underlying.onBeginList();
    }

    @Override
    public void onListItem() {
        if (depth == 0) {
            onNewItem();
        } else {
            assert underlying != null;
            underlying.onListItem();
        }
    }

    @Override
    public void onEndList() {
        assert underlying != null;
        underlying.onEndList();
        depth--;
    }

    @Override
    public void onBeginAttributes() {
        assert underlying != null;
        depth++;
        underlying.onBeginAttributes();
    }

    @Override
    public void onEndAttributes() {
        assert underlying != null;
        depth--;
        underlying.onEndAttributes();
    }

    @Override
    public void onBeginMap() {
        assert underlying != null;
        depth++;
        underlying.onBeginMap();
    }

    @Override
    public void onEndMap() {
        assert underlying != null;
        underlying.onBeginMap();
        depth--;
    }

    @Override
    public void onKeyedItem(@Nonnull byte[] value, int offset, int length) {
        if (depth == 0) {
            onNewItem();
        } else {
            assert underlying != null;
            underlying.onKeyedItem(value, offset, length);
        }
    }

    private void onNewItem() {
        if (depth != 0) {
            throw new IllegalStateException();
        }

        if (underlying != null) {
            underlying.close();
        }
        if (first) {
            first = false;
        } else {
            stringBuilder.append(';');
        }
        underlying = new YsonTextWriter(stringBuilder);
    }

    @Override
    public void close() {
        if (depth != 0) {
            throw new IllegalStateException();
        }
        if (underlying != null) {
            underlying.close();
        }
    }
}
