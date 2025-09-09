package tech.ytsaurus.yson;

import java.io.StringWriter;
import java.util.function.Function;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class YsonTextWriterTest {
    @Test
    public void testOnEntity() {
        Function<YsonConsumer, Void> entityBuilder = (YsonConsumer consumer) -> {
            consumer.onEntity();
            return null;
        };
        assertEquals(
                "#",
                buildTextYson(entityBuilder)
        );
        assertEquals(
                "#",
                buildPrettyTextYson(entityBuilder)
        );
    }

    @Test
    public void testOnInteger() {
        Function<YsonConsumer, Void> builder42 = (YsonConsumer consumer) -> {
            consumer.onInteger(42);
            return null;
        };
        assertEquals(
                "42",
                buildTextYson(builder42)
        );
        assertEquals(
                "42",
                buildPrettyTextYson(builder42)
        );

        Function<YsonConsumer, Void> builderMinusOne = (YsonConsumer consumer) -> {
            consumer.onInteger(-1);
            return null;
        };
        assertEquals(
                "-1",
                buildTextYson(builderMinusOne)
        );
        assertEquals(
                "-1",
                buildPrettyTextYson(builderMinusOne)
        );
    }

    @Test
    public void testOnUnsignedInteger() {
        Function<YsonConsumer, Void> unsignedBuilder = (YsonConsumer consumer) -> {
            consumer.onUnsignedInteger(100500);
            return null;
        };
        assertEquals(
                "100500u",
                buildTextYson(unsignedBuilder)
        );
        assertEquals(
                "100500u",
                buildPrettyTextYson(unsignedBuilder)
        );

        Function<YsonConsumer, Void> hugeUnsignedBuilder = (YsonConsumer consumer) -> {
            consumer.onUnsignedInteger(-1);
            return null;
        };
        assertEquals(
                "18446744073709551615u",
                buildTextYson(hugeUnsignedBuilder)
        );
        assertEquals(
                "18446744073709551615u",
                buildPrettyTextYson(hugeUnsignedBuilder)
        );
    }

    @Test
    public void testOnBoolean() {
        Function<YsonConsumer, Void> trueBuilder = (YsonConsumer consumer) -> {
            consumer.onBoolean(true);
            return null;
        };
        assertEquals(
                "%true",
                buildTextYson(trueBuilder)
        );
        assertEquals(
                "%true",
                buildPrettyTextYson(trueBuilder)
        );

        Function<YsonConsumer, Void> falseBuilder = (YsonConsumer consumer) -> {
            consumer.onBoolean(false);
            return null;
        };
        assertEquals(
                "%false",
                buildTextYson(falseBuilder)
        );
        assertEquals(
                "%false",
                buildPrettyTextYson(falseBuilder)
        );
    }

    @Test
    public void testOnDouble() {
        Function<YsonConsumer, Void> doubleBuilder = (YsonConsumer consumer) -> {
            consumer.onDouble(2.125);
            return null;
        };
        assertEquals(
                "2.125",
                buildTextYson(doubleBuilder)
        );
        assertEquals(
                "2.125",
                buildPrettyTextYson(doubleBuilder)
        );

        Function<YsonConsumer, Void> nanBuilder = (YsonConsumer consumer) -> {
            consumer.onDouble(Double.NaN);
            return null;
        };
        assertEquals(
                "%nan",
                buildTextYson(nanBuilder)
        );
        assertEquals(
                "%nan",
                buildPrettyTextYson(nanBuilder)
        );
    }

    @Test
    public void testOnString() {
        Function<YsonConsumer, Void> stringBuilder = (YsonConsumer consumer) -> {
            consumer.onString("foo");
            return null;
        };
        assertEquals(
                "\"foo\"",
                buildTextYson(stringBuilder)
        );
        assertEquals(
                "\"foo\"",
                buildPrettyTextYson(stringBuilder)
        );

        Function<YsonConsumer, Void> binStringBuilder = (YsonConsumer consumer) -> {
            byte[] data = {0, 127, -8};
            consumer.onString(data, 0, data.length);
            return null;
        };
        assertEquals(
                "\"\\x00\\x7f\\xf8\"",
                buildTextYson(binStringBuilder)
        );
        assertEquals(
                "\"\\x00\\x7f\\xf8\"",
                buildPrettyTextYson(binStringBuilder)
        );
    }

    @Test
    public void testList() {
        Function<YsonConsumer, Void> listBuilder = (YsonConsumer consumer) -> {
            consumer.onBeginList();

            consumer.onListItem();
            consumer.onString("foo");

            consumer.onListItem();
            consumer.onInteger(42);

            consumer.onListItem();
            consumer.onEntity();

            consumer.onEndList();

            return null;
        };
        assertEquals(
                "[\"foo\";42;#;]",
                buildTextYson(listBuilder)
        );
        assertEquals(
                "[\n" +
                "    \"foo\";\n" +
                "    42;\n" +
                "    #;\n" +
                "]",
                buildPrettyTextYson(listBuilder)
        );

        Function<YsonConsumer, Void> multiListBuilder = (YsonConsumer consumer) -> {
            consumer.onBeginList();

            consumer.onListItem();

            consumer.onBeginList();

            consumer.onListItem();
            consumer.onInteger(1);

            consumer.onListItem();
            consumer.onInteger(2);

            consumer.onListItem();
            consumer.onInteger(3);

            consumer.onEndList();

            consumer.onListItem();

            consumer.onBeginList();

            consumer.onListItem();
            consumer.onInteger(4);

            consumer.onListItem();

            consumer.onBeginList();
            consumer.onListItem();
            consumer.onInteger(5);
            consumer.onEndList();

            consumer.onEndList();

            consumer.onEndList();

            return null;
        };
        assertEquals(
                "[[1;2;3;];[4;[5;];];]",
                buildTextYson(multiListBuilder)
        );

        assertEquals(
                "[\n" +
                "    [\n" +
                "        1;\n" +
                "        2;\n" +
                "        3;\n" +
                "    ];\n" +
                "    [\n" +
                "        4;\n" +
                "        [\n" +
                "            5;\n" +
                "        ];\n" +
                "    ];\n" +
                "]",
                buildPrettyTextYson(multiListBuilder)
        );

        Function<YsonConsumer, Void> listOfEmptyListsBuilder = (YsonConsumer consumer) -> {
            consumer.onBeginList();
            consumer.onListItem();

            consumer.onBeginList();
            consumer.onEndList();

            consumer.onListItem();

            consumer.onBeginList();
            consumer.onEndList();

            consumer.onEndList();
            return null;
        };
        assertEquals(
                "[[];[];]",
                buildTextYson(listOfEmptyListsBuilder)
        );
        assertEquals(
                "[\n" +
                "    [];\n" +
                "    [];\n" +
                "]",
                buildPrettyTextYson(listOfEmptyListsBuilder)
        );
    }

    @Test
    public void testMap() {
        Function<YsonConsumer, Void> mapBuilder = (YsonConsumer consumer) -> {
            consumer.onBeginMap();

            consumer.onKeyedItem("foo");
            consumer.onString("bar");

            byte[] k2 = {-4, 62, 12};
            consumer.onKeyedItem(k2, 0, k2.length);
            consumer.onInteger(-5);

            consumer.onEndMap();
            return null;
        };
        assertEquals(
                "{\"foo\"=\"bar\";\"\\xfc>\\x0c\"=-5;}",
                buildTextYson(mapBuilder)
        );
        assertEquals(
                "{\n" +
                "    \"foo\" = \"bar\";\n" +
                "    \"\\xfc>\\x0c\" = -5;\n" +
                "}",
                buildPrettyTextYson(mapBuilder)
        );

        Function<YsonConsumer, Void> mapOfMapsBuilder = (YsonConsumer consumer) -> {
            consumer.onBeginMap();

            consumer.onKeyedItem("42");
            consumer.onInteger(42);

            consumer.onKeyedItem("62");

            consumer.onBeginMap();
            consumer.onKeyedItem("6");
            consumer.onInteger(6);

            consumer.onKeyedItem("2");
            consumer.onInteger(2);
            consumer.onEndMap();

            consumer.onKeyedItem("");

            consumer.onBeginMap();
            consumer.onEndMap();

            consumer.onEndMap();
            return null;
        };
        assertEquals(
                "{\"42\"=42;\"62\"={\"6\"=6;\"2\"=2;};\"\"={};}",
                buildTextYson(mapOfMapsBuilder)
        );
        assertEquals(
                "{\n" +
                "    \"42\" = 42;\n" +
                "    \"62\" = {\n" +
                "        \"6\" = 6;\n" +
                "        \"2\" = 2;\n" +
                "    };\n" +
                "    \"\" = {};\n" +
                "}",
                buildPrettyTextYson(mapOfMapsBuilder)
        );

        Function<YsonConsumer, Void> mapOfMapsAndListBuilder = (YsonConsumer consumer) -> {
            consumer.onBeginMap();

            consumer.onKeyedItem("42");
            consumer.onInteger(42);

            consumer.onKeyedItem("62");

            consumer.onBeginMap();
            consumer.onKeyedItem("6");
            consumer.onInteger(6);

            consumer.onKeyedItem("2");
            consumer.onInteger(2);

            consumer.onKeyedItem("list");
            consumer.onBeginList();

            consumer.onListItem();
            consumer.onInteger(4);

            consumer.onListItem();

            consumer.onBeginList();
            consumer.onListItem();
            consumer.onInteger(5);
            consumer.onEndList();

            consumer.onEndList();

            consumer.onEndMap();

            consumer.onKeyedItem("");

            consumer.onBeginMap();
            consumer.onEndMap();

            consumer.onEndMap();
            return null;
        };
        assertEquals(
                "{\"42\"=42;\"62\"={\"6\"=6;\"2\"=2;\"list\"=[4;[5;];];};\"\"={};}",
                buildTextYson(mapOfMapsAndListBuilder)
        );
        assertEquals(
                "{\n" +
                "    \"42\" = 42;\n" +
                "    \"62\" = {\n" +
                "        \"6\" = 6;\n" +
                "        \"2\" = 2;\n" +
                "        \"list\" = [\n" +
                "            4;\n" +
                "            [\n" +
                "                5;\n" +
                "            ];\n" +
                "        ];\n" +
                "    };\n" +
                "    \"\" = {};\n" +
                "}",
                buildPrettyTextYson(mapOfMapsAndListBuilder)
        );

    }

    @Test
    public void testIndent() {
        StringWriter writer = new StringWriter();
        YsonTextWriter consumer = YsonTextWriter.builder()
                .setWriter(writer)
                .setPrettyPrinting()
                .setIndent(8)
                .build();
        consumer.onBeginMap();
        consumer.onKeyedItem("foo");
        consumer.onString("bar");
        consumer.onKeyedItem("33");
        consumer.onInteger(33);
        consumer.onEndMap();
        consumer.close();
        String result = writer.toString();
        assertEquals(
                "{\n" +
                "        \"foo\" = \"bar\";\n" +
                "        \"33\" = 33;\n" +
                "}",
                result
        );
    }

    @Test
    public void testZeroIndent() {
        StringWriter writer = new StringWriter();
        YsonTextWriter consumer = YsonTextWriter.builder()
                .setWriter(writer)
                .setPrettyPrinting()
                .setIndent(0)
                .build();
        consumer.onBeginMap();
        consumer.onKeyedItem("foo");
        consumer.onString("bar");
        consumer.onKeyedItem("33");
        consumer.onInteger(33);
        consumer.onEndMap();
        consumer.close();
        String result = writer.toString();
        assertEquals(
                "{\n" +
                "\"foo\" = \"bar\";\n" +
                "\"33\" = 33;\n" +
                "}",
                result
        );
    }

    @Test
    public void testNegativeIndentError() {
        StringWriter writer = new StringWriter();
        assertThrows(IllegalArgumentException.class, () -> YsonTextWriter.builder()
                .setWriter(writer)
                .setPrettyPrinting()
                .setIndent(-1));
    }

    @Test
    public void testAttributes() {
        Function<YsonConsumer, Void> attributesBuilder = (YsonConsumer consumer) -> {
            consumer.onBeginAttributes();

            consumer.onKeyedItem("1");
            consumer.onUnsignedInteger(1);

            consumer.onKeyedItem("2");
            consumer.onUnsignedInteger(2);

            consumer.onKeyedItem("3");
            consumer.onUnsignedInteger(3);

            consumer.onEndAttributes();
            consumer.onString("skiff");

            return null;
        };
        assertEquals(
                "<\"1\"=1u;\"2\"=2u;\"3\"=3u;>\"skiff\"",
                buildTextYson(attributesBuilder)
        );
        assertEquals(
                "<\n" +
                "    \"1\" = 1u;\n" +
                "    \"2\" = 2u;\n" +
                "    \"3\" = 3u;\n" +
                "> \"skiff\"",
                buildPrettyTextYson(attributesBuilder)
        );

        Function<YsonConsumer, Void> moreAttributesBuilder = (YsonConsumer consumer) -> {
            consumer.onBeginAttributes();

            consumer.onKeyedItem("1");
            consumer.onBeginAttributes();

            consumer.onKeyedItem("a");
            consumer.onInteger(1);

            consumer.onKeyedItem("b");
            consumer.onInteger(2);

            consumer.onEndAttributes();
            consumer.onUnsignedInteger(1);

            consumer.onKeyedItem("2");
            consumer.onBeginList();
            consumer.onEndList();

            consumer.onKeyedItem("3");
            consumer.onBeginMap();
            consumer.onEndMap();


            consumer.onEndAttributes();
            consumer.onString("skiff");

            return null;
        };
        assertEquals(
                "<\"1\"=<\"a\"=1;\"b\"=2;>1u;\"2\"=[];\"3\"={};>\"skiff\"",
                buildTextYson(moreAttributesBuilder)
        );
        assertEquals(
                "<\n" +
                "    \"1\" = <\n" +
                "        \"a\" = 1;\n" +
                "        \"b\" = 2;\n" +
                "    > 1u;\n" +
                "    \"2\" = [];\n" +
                "    \"3\" = {};\n" +
                "> \"skiff\"",
                buildPrettyTextYson(moreAttributesBuilder)
        );
    }

    String buildTextYson(Function<YsonConsumer, Void> builder) {
        StringWriter writer = new StringWriter();
        YsonTextWriter consumer = YsonTextWriter.builder()
                .setWriter(writer)
                .build();
        builder.apply(consumer);
        consumer.close();
        return writer.toString();
    }

    String buildPrettyTextYson(Function<YsonConsumer, Void> builder) {
        StringWriter writer = new StringWriter();
        YsonTextWriter consumer = YsonTextWriter.builder()
                .setWriter(writer)
                .setPrettyPrinting()
                .build();
        builder.apply(consumer);
        consumer.close();
        return writer.toString();
    }
}
