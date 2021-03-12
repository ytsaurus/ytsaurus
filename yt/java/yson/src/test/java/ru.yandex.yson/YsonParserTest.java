package ru.yandex.yson;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.StringReader;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

import javax.annotation.Nonnull;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThrows;

@RunWith(Parameterized.class)
public class YsonParserTest {
    final String testType;

    @Parameterized.Parameters(name = "{0}")
    public static Collection<String> testTypes() {
        return List.of("stream", "bytes");
    }

    public YsonParserTest(String testType) {
        this.testType = testType;
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
        assertThat(unescapeBytes("ab\\x5Fcd\\xf5"), is(new byte[]{'a', 'b', 0x5f, 'c', 'd', -11}));
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
        assertThat(canonizeNode("#"), is("#"));
    }

    @Test
    public void testBinaryString() {
        YsonError e;

        assertThat(canonizeNode("\\x01\\x00"), is("\"\""));
        assertThat(canonizeNode("\\x01\\x06foo"), is("\"foo\""));

        e = assertThrows(YsonError.class, () -> canonizeNode("\\x01\\x03foo"));
        assertThat(e.getMessage(), containsString("Yson string length is negative"));

        e = assertThrows(YsonError.class, () -> canonizeNode("\\x01\\x84\\x85\\x86\\x87\\x88\\x89\\x02foo"));
        assertThat(e.getMessage(), containsString("Yson string length exceeds limit"));

        e = assertThrows(YsonError.class, () -> canonizeNode("\\x01\\x08foo"));
        assertThat(e.getMessage(), containsString("Unexpected end of stream"));

        e = assertThrows(YsonError.class, () -> canonizeNode("\\x01\\x82"));
        assertThat(e.getMessage(), containsString("Unexpected end of stream"));
    }

    @Test
    public void testBinaryInt64() {
        assertThat(canonizeNode("\\x02\\x00"), is("0"));
        assertThat(canonizeNode("\\x02\\x01"), is("-1"));
        assertThat(canonizeNode("\\x02\\x02"), is("1"));
        assertThat(canonizeNode("\\x02\\x06"), is("3"));
        assertThat(canonizeNode("\\x02\\x84\\x85\\x86\\x87\\x88\\x89\\x02"), is("4553746465090"));
        assertThat(
                canonizeNode("\\x02\\x81\\xd5\\xf4\\xd9\\xea\\xa4\\xbd\\xd3\\x95\\x01"),
                is("-5391787952107853121"));

        assertThrows(YsonError.class,
                () -> canonizeNode("\\x02\\x84\\x85\\x86\\x87\\x88\\x89\\x90\\x91\\x92\\x93\\x72"));
        assertThrows(YsonError.class,
                () -> canonizeNode("\\x02\\x84\\x85\\x86\\x87\\x88\\x89\\x90\\x91\\x92\\x93"));

    }

    @Test
    public void testBinaryUint64() {
        assertThat(canonizeNode("\\x06\\x00"), is("0u"));
        assertThat(canonizeNode("\\x06\\x01"), is("1u"));
        assertThat(canonizeNode("\\x06\\x02"), is("2u"));
        assertThat(canonizeNode("\\x06\\x06"), is("6u"));
        assertThat(canonizeNode("\\x06\\x84\\x85\\x86\\x87\\x88\\x89\\x02"), is("9107492930180u"));

        assertThrows(YsonError.class,
                () -> canonizeNode("\\x06\\x84\\x85\\x86\\x87\\x88\\x89\\x90\\x91\\x92\\x93\\x72"));
        assertThrows(YsonError.class,
                () -> canonizeNode("\\x06\\x84\\x85\\x86\\x87\\x88\\x89\\x90\\x91\\x92\\x93"));
    }

    @Test
    public void testBinaryDouble() {
        assertThat(canonizeNode("\\x03\\x00\\x00\\x00\\x00\\x00\\x00\\x0A\\x40"), is("3.25"));
        assertThat(canonizeNode("\\x03\\x00\\x00\\x00\\x00\\x00\\x00\\xc0\\xbf"), is("-0.125"));
        assertThat(canonizeNode("\\x03\\x00\\x00\\x00\\x00\\x00\\x00\\xf8\\x7f"), is("%nan"));
    }

    @Test
    public void testBinaryBoolean() {
        assertThat(canonizeNode("\\x04"), is("%false"));
        assertThat(canonizeNode("\\x05"), is("%true"));
    }

    @Test
    public void testUnquotedString() {
        assertThat(canonizeNode("foo"), is("\"foo\""));
        assertThat(canonizeNode("bar25"), is("\"bar25\""));
        assertThat(canonizeNode("_"), is("\"_\""));
        assertThat(canonizeNode("foo.bar"), is("\"foo.bar\""));
    }

    @Test
    public void testQuotedString() {
        assertThat(canonizeNode("\"foo\""), is("\"foo\""));
        assertThat(canonizeNode("\"25\""), is("\"25\""));
        assertThat(canonizeNode("\"foo\""), is("\"foo\""));
        assertThat(canonizeNode("\"\\\\000\""), is("\"\\x00\""));
        assertThat(canonizeNode("\"\\\\102\""), is("\"B\""));
        assertThat(canonizeNode("\"\\\\377\""), is("\"\\xff\""));
        assertThat(canonizeNode("\"\\\\x42\""), is("\"B\""));

        assertThrows(YsonError.class,
                () -> canonizeNode("\"\\\\400\""));
        assertThrows(YsonError.class,
                () -> canonizeNode("\"\\\\x8\""));
    }

    @Test
    public void testTextBoolean() {
        assertThat(canonizeNode("%true"), is("%true"));
        assertThat(canonizeNode("%false"), is("%false"));
    }

    @Test
    public void testTextInt64() {
        assertThat(canonizeNode("-100500"), is("-100500"));
        assertThat(canonizeNode("100500"), is("100500"));
        assertThat(canonizeNode("+100500"), is("100500"));
        assertThat(canonizeNode("+0"), is("0"));
        assertThat(canonizeNode("-0"), is("0"));
        assertThrows(YsonError.class,
                () -> canonizeNode("10050000000000000000000000000000000000000000000000"));
    }

    @Test
    public void testTextUint64() {
        assertThat(canonizeNode("100500u"), is("100500u"));
        assertThat(canonizeNode("+100500u"), is("100500u"));
        assertThat(canonizeNode("+0u"), is("0u"));
        assertThrows(YsonError.class,
                () -> canonizeNode("-100500u"));
        assertThrows(YsonError.class,
                () -> canonizeNode("10050000000000000000000000000000000000000000000000u"));
    }

    @Test
    public void testTextDouble() {
        assertThat(canonizeNode("3125e-3"), is("3.125"));
        assertThat(canonizeNode(".125"), is("0.125"));
        assertThat(canonizeNode(" %nan "), is("%nan"));
        assertThat(canonizeNode("%inf"), is("%+inf"));
        assertThat(canonizeNode("%+inf"), is("%+inf"));
        assertThat(canonizeNode("%-inf"), is("%-inf"));

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
        assertThat(canonizeNode("[]"), is("[]"));
        assertThat(canonizeNode("[ ]"), is("[]"));
        assertThat(canonizeNode("[1;2;3]"), is("[1;2;3]"));
        assertThat(canonizeNode("[ 1 ; 2 ; 3 ]"), is("[1;2;3]"));

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
        assertThat(canonizeNode("{}"), is("{}"));
        assertThat(canonizeNode("{ }"), is("{}"));
        assertThat(canonizeNode("{foo=bar}"), is("{\"foo\"=\"bar\"}"));
        assertThat(canonizeNode("{foo=bar;}"), is("{\"foo\"=\"bar\"}"));
        assertThat(canonizeNode("{foo=bar;bar=baz}"), is("{\"foo\"=\"bar\";\"bar\"=\"baz\"}"));
        assertThat(canonizeNode(" { foo = bar ; bar = 42 ; } "), is("{\"foo\"=\"bar\";\"bar\"=42}"));

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
        assertThat(canonizeNode("<>42"), is("<>42"));
        assertThat(canonizeNode("<foo=bar>42"), is("<\"foo\"=\"bar\">42"));
        assertThat(canonizeNode("<foo=bar;>42"), is("<\"foo\"=\"bar\">42"));
        assertThat(canonizeNode("<foo=<baz=32>bar;>42"), is("<\"foo\"=<\"baz\"=32>\"bar\">42"));
        assertThat(canonizeNode(" < foo = < baz = 32 > bar ; > 42"), is("<\"foo\"=<\"baz\"=32>\"bar\">42"));

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
        assertThat(canonizeNode(
                "[123;\\x02\\x82\\x06;{\\x01\\x02a=\\x02\\x84\\x34;\\x01\\x06foo=<bar=\"baz \">\\x05}]"
        ), is("[123;385;{\"a\"=3330;\"foo\"=<\"bar\"=\"baz \">%true}]"));
    }

    @Test
    public void testBufferBoundaries() {
        final String textData = "[123;\\x02\\x82\\x06;{\\x01\\x02a=\\x02\\x84\\x34;\\x01\\x06foo=<bar=\"baz \">\\x05};" +
                "\\x06\\x80\\x80\\x84\\x85\\x86\\x87\\x88\\x89\\x02;" +
                "\\x03\\x00\\x00\\x00\\x00\\x00\\x00\\xc0\\xbf" +
                "]";
        final String expectedCanonized = "[123;385;{\"a\"=3330;\"foo\"=<\"bar\"=\"baz \">%true};149217164168069120u;-0.125]";
        byte[] binaryData = unescapeBytes(textData);
        for (int bufferSize = 1; bufferSize != binaryData.length; ++bufferSize) {
            StringBuilder result = new StringBuilder();
            YsonParser parser = new YsonParser(new ByteArrayInputStream(binaryData), bufferSize);
            try (YsonTextWriter textWriter = new YsonTextWriter(result)) {
                parser.parseNode(textWriter);
            }
            assertThat(result.toString(), is(expectedCanonized));
        }
    }

    @Test
    public void testListFragment() {
        assertThat(canonizeListFragment(""), is(""));
        assertThat(canonizeListFragment(" "), is(""));
        assertThat(canonizeListFragment("1;2;3"), is("1;2;3"));
        assertThat(canonizeListFragment("1 ; 2 ; 3"), is("1;2;3"));

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

        assertThat(nextItem.get(), is("123"));
        assertThat(nextItem.get(), is("385"));
        assertThat(nextItem.get(), is("{\"a\"=3330;\"foo\"=<\"bar\"=\"baz \">%true}"));
        assertThat(nextItem.get(), is("149217164168069120u"));
        assertThat(nextItem.get(), is(nullValue()));

    }
}

class FragmentYsonTextWriter  implements ClosableYsonConsumer {
    StringBuilder stringBuilder;
    ClosableYsonConsumer underlying;
    int depth = 0;
    boolean first = true;

    public FragmentYsonTextWriter(StringBuilder sb) {
        stringBuilder = sb;
    }

    @Override
    public void onEntity() {
        underlying.onEntity();
    }

    @Override
    public void onInteger(long value) {
        underlying.onInteger(value);
    }

    @Override
    public void onUnsignedInteger(long value) {
        underlying.onUnsignedInteger(value);
    }

    @Override
    public void onBoolean(boolean value) {
        underlying.onBoolean(value);
    }

    @Override
    public void onDouble(double value) {
        underlying.onDouble(value);
    }

    @Override
    public void onString(@Nonnull byte[] value, int offset, int length) {
        underlying.onString(value, offset, length);
    }

    @Override
    public void onBeginList() {
        depth++;
        underlying.onBeginList();
    }

    @Override
    public void onListItem() {
        if (depth == 0) {
            onNewItem();
        } else {
            underlying.onListItem();
        }
    }

    @Override
    public void onEndList() {
        underlying.onEndList();
        depth--;
    }

    @Override
    public void onBeginAttributes() {
        depth++;
        underlying.onBeginAttributes();
    }

    @Override
    public void onEndAttributes() {
        depth--;
        underlying.onEndAttributes();
    }

    @Override
    public void onBeginMap() {
        depth++;
        underlying.onBeginMap();
    }

    @Override
    public void onEndMap() {
        underlying.onBeginMap();
        depth--;
    }

    @Override
    public void onKeyedItem(@Nonnull byte[] value, int offset, int length) {
        if (depth == 0) {
            onNewItem();
        } else {
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
