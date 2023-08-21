package tech.ytsaurus.yson;

import java.io.StringWriter;
import java.util.function.Function;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class YsonTextWriterTest {
    @Test
    public void testOnEntity() {
         assertEquals(
                buildTextYson((YsonConsumer consumer) -> {
                    consumer.onEntity();
                    return null;
                }),
                "#"
        );
    }

    @Test
    public void testOnInteger() {
         assertEquals(
                buildTextYson((YsonConsumer consumer) -> {
                    consumer.onInteger(42);
                    return null;
                }),
                "42"
        );

         assertEquals(
                buildTextYson((YsonConsumer consumer) -> {
                    consumer.onInteger(-1);
                    return null;
                }),
                "-1"
        );
    }

    @Test
    public void testOnUnsignedInteger() {
         assertEquals(
                buildTextYson((YsonConsumer consumer) -> {
                    consumer.onUnsignedInteger(100500);
                    return null;
                }),
                "100500u"
        );

         assertEquals(
                buildTextYson((YsonConsumer consumer) -> {
                    consumer.onUnsignedInteger(-1);
                    return null;
                }),
                "18446744073709551615u"
        );
    }

    @Test
    public void testOnBoolean() {
         assertEquals(
                buildTextYson((YsonConsumer consumer) -> {
                    consumer.onBoolean(true);
                    return null;
                }),
                "%true"
        );

         assertEquals(
                buildTextYson((YsonConsumer consumer) -> {
                    consumer.onBoolean(false);
                    return null;
                }),
                "%false"
        );
    }

    @Test
    public void testOnDouble() {
         assertEquals(
                buildTextYson((YsonConsumer consumer) -> {
                    consumer.onDouble(2.125);
                    return null;
                }),
                "2.125"
        );

         assertEquals(
                buildTextYson((YsonConsumer consumer) -> {
                    consumer.onDouble(Double.NaN);
                    return null;
                }),
                "%nan"
        );
    }

    @Test
    public void testOnString() {
         assertEquals(
                buildTextYson((YsonConsumer consumer) -> {
                    consumer.onString("foo");
                    return null;
                }),
                "\"foo\""
        );

         assertEquals(
                buildTextYson((YsonConsumer consumer) -> {
                    byte[] data = {0, 127, -8};
                    consumer.onString(data, 0, data.length);
                    return null;
                }),
                "\"\\x00\\x7f\\xf8\""
        );
    }

    @Test
    public void testList() {
         assertEquals(
                buildTextYson((YsonConsumer consumer) -> {
                    consumer.onBeginList();

                    consumer.onListItem();
                    consumer.onString("foo");

                    consumer.onListItem();
                    consumer.onInteger(42);

                    consumer.onListItem();
                    consumer.onEntity();

                    consumer.onEndList();

                    return null;
                }),
                "[\"foo\";42;#;]"
        );

         assertEquals(
                buildTextYson((YsonConsumer consumer) -> {
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
                }),
                "[[1;2;3;];[4;[5;];];]"
        );

         assertEquals(
                buildTextYson((YsonConsumer consumer) -> {
                    consumer.onBeginList();
                    consumer.onListItem();

                    consumer.onBeginList();
                    consumer.onEndList();

                    consumer.onListItem();

                    consumer.onBeginList();
                    consumer.onEndList();

                    consumer.onEndList();
                    return null;
                }),
                "[[];[];]"
        );
    }

    @Test
    public void testMap() {
         assertEquals(
                buildTextYson((YsonConsumer consumer) -> {
                    consumer.onBeginMap();

                    consumer.onKeyedItem("foo");
                    consumer.onString("bar");

                    byte[] k2 = {-4, 62, 12};
                    consumer.onKeyedItem(k2, 0, k2.length);
                    consumer.onInteger(-5);

                    consumer.onEndMap();
                    return null;
                }),
                "{\"foo\"=\"bar\";\"\\xfc>\\x0c\"=-5;}"
        );

         assertEquals(
                buildTextYson((YsonConsumer consumer) -> {
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
                }),
                "{\"42\"=42;\"62\"={\"6\"=6;\"2\"=2;};\"\"={};}"
        );
    }

    @Test
    public void testAttributes() {
         assertEquals(
                buildTextYson((YsonConsumer consumer) -> {
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
                }),
                "<\"1\"=1u;\"2\"=2u;\"3\"=3u;>\"skiff\""
        );

         assertEquals(
                buildTextYson((YsonConsumer consumer) -> {
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
                }),
                "<\"1\"=<\"a\"=1;\"b\"=2;>1u;\"2\"=[];\"3\"={};>\"skiff\""
        );
    }

    String buildTextYson(Function<YsonConsumer, Void> builder) {
        StringWriter writer = new StringWriter();
        YsonTextWriter consumer = new YsonTextWriter(writer);
        builder.apply(consumer);
        consumer.close();
        return writer.toString();
    }
}
