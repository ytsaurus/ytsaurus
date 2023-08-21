package tech.ytsaurus.yson;

import java.io.ByteArrayOutputStream;
import java.util.Random;
import java.util.function.Function;

import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;

public class YsonBinaryWriterTest {
    @Test
    public void testOnEntity() {
        assertArrayEquals(
                buildBinaryYson((YsonConsumer consumer) -> {
                    consumer.onEntity();
                    return null;
                }),
                new byte[]{'#'}
        );
    }

    @Test
    public void testOnInteger() {
        assertArrayEquals(
                buildBinaryYson((YsonConsumer consumer) -> {
                    consumer.onInteger(42);
                    return null;
                }),
                new byte[]{2, 84}
        );

        assertArrayEquals(
                buildBinaryYson((YsonConsumer consumer) -> {
                    consumer.onInteger(-1);
                    return null;
                }),
                new byte[]{2, 1}
        );

        assertArrayEquals(
                buildBinaryYson((YsonConsumer consumer) -> {
                    consumer.onInteger(100500);
                    return null;
                }),
                new byte[]{2, -88, -94, 12}
        );
    }

    @Test
    public void testOnUnsignedInteger() {
        assertArrayEquals(
                buildBinaryYson((YsonConsumer consumer) -> {
                    consumer.onUnsignedInteger(100500);
                    return null;
                }),
                new byte[]{6, -108, -111, 6}
        );

        assertArrayEquals(
                buildBinaryYson((YsonConsumer consumer) -> {
                    consumer.onUnsignedInteger(-1);
                    return null;
                }),
                new byte[]{6, -1, -1, -1, -1, -1, -1, -1, -1, -1, 1}
        );
    }

    @Test
    public void testOnBoolean() {
        assertArrayEquals(
                buildBinaryYson((YsonConsumer consumer) -> {
                    consumer.onBoolean(true);
                    return null;
                }),
                new byte[]{5}
        );

        assertArrayEquals(
                buildBinaryYson((YsonConsumer consumer) -> {
                    consumer.onBoolean(false);
                    return null;
                }),
                new byte[]{4}
        );
    }

    @Test
    public void testOnDouble() {
        assertArrayEquals(
                buildBinaryYson((YsonConsumer consumer) -> {
                    consumer.onDouble(2.125);
                    return null;
                }),
                new byte[]{3, 0, 0, 0, 0, 0, 0, 1, 64}
        );

        assertArrayEquals(
                buildBinaryYson((YsonConsumer consumer) -> {
                    consumer.onDouble(Double.NaN);
                    return null;
                }),
                // NB onDouble relies on Double.doubleToLongBits method, that
                // coverts all nan numbers to the same bit representation
                new byte[]{3, 0, 0, 0, 0, 0, 0, -8, 127}
        );
    }

    @Test
    public void testOnString() {
        assertArrayEquals(
                buildBinaryYson((YsonConsumer consumer) -> {
                    consumer.onString("foo");
                    return null;
                }),
                new byte[]{1, 6, 'f', 'o', 'o'}
        );

        assertArrayEquals(
                buildBinaryYson((YsonConsumer consumer) -> {
                    byte[] data = {0, 127, -8, -42};
                    consumer.onString(data, 0, data.length);
                    return null;
                }),
                new byte[]{1, 8, 0, 127, -8, -42}
        );
    }

    @Test
    public void testList() {
        assertArrayEquals(
                buildBinaryYson((YsonConsumer consumer) -> {
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
                new byte[]{'[', 1, 6, 'f', 'o', 'o', ';', 2, 84, ';', '#', ']'}
        );

        assertArrayEquals(
                buildBinaryYson((YsonConsumer consumer) -> {
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
                new byte[] {
                        '[', '[', 2, 2, ';', 2, 4, ';', 2, 6, ']', ';', '[', 2, 8, ';', '[', 2, 10, ']', ']', ']'
                }
        );

        assertArrayEquals(
                buildBinaryYson((YsonConsumer consumer) -> {
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
                new byte[]{'[', '[', ']', ';', '[', ']', ']'}
        );
    }

    @Test
    public void testMap() {
        assertArrayEquals(
                buildBinaryYson((YsonConsumer consumer) -> {
                    consumer.onBeginMap();

                    consumer.onKeyedItem("foo");
                    consumer.onString("bar");

                    byte[] k2 = {-4, 62, 12};
                    consumer.onKeyedItem(k2, 0, k2.length);
                    consumer.onInteger(-5);

                    consumer.onEndMap();
                    return null;
                }),
                new byte[] {
                        '{', 1, 6, 'f', 'o', 'o', '=', 1, 6, 'b', 'a', 'r', ';', 1, 6, -4, 62, 12, '=', 2, 9, '}'
                }
        );

        assertArrayEquals(
                buildBinaryYson((YsonConsumer consumer) -> {
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
                new byte[]{
                        '{', 1, 4, '4', '2', '=', 2, 84, ';',
                        1, 4, '6', '2', '=', '{', 1, 2, '6', '=', 2, 12, ';', 1, 2, '2', '=', 2, 4, '}', ';',
                        1, 0, '=', '{', '}', '}'
                }
        );
    }

    @Test
    public void testAttributes() {
        assertArrayEquals(
                buildBinaryYson((YsonConsumer consumer) -> {
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
                new byte[]{
                        '<', 1, 2, '1', '=', 6, 1, ';', 1, 2, '2', '=', 6, 2, ';', 1, 2, '3', '=', 6, 3, '>',
                        1, 10, 's', 'k', 'i', 'f', 'f'

                }
        );

        assertArrayEquals(
                buildBinaryYson((YsonConsumer consumer) -> {
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
                new byte[]{
                        '<', 1, 2, '1', '=', '<', 1, 2, 'a', '=', 2, 2, ';', 1, 2, 'b', '=', 2, 4, '>', 6, 1, ';',
                        1, 2, '2', '=', '[', ']', ';', 1, 2, '3', '=', '{', '}', '>', 1, 10, 's', 'k', 'i', 'f', 'f'
                }
        );
    }

    @Test
    public void testBigYson() {
        buildBinaryYson(
                (YsonConsumer consumer) -> {
                    Random rnd = new Random(42);
                    consumer.onBeginList();
                    for (int i = 0; i < 100000; ++i) {
                        int type = rnd.nextInt(6);
                        consumer.onListItem();
                        switch (type) {
                            case 0:
                                consumer.onEntity();
                                break;
                            case 1:
                                consumer.onInteger(rnd.nextLong());
                                break;
                            case 2:
                                consumer.onUnsignedInteger(rnd.nextLong());
                                break;
                            case 3:
                                consumer.onDouble(rnd.nextDouble());
                                break;
                            case 4:
                                consumer.onBoolean(rnd.nextBoolean());
                                break;
                            case 5:
                                byte[] b = new byte[rnd.nextInt(128)];
                                rnd.nextBytes(b);
                                consumer.onString(b, 0, b.length);
                                break;
                            default:
                                throw new RuntimeException("unreachable");
                        }
                    }
                    consumer.onEndList();
                    return null;
                }
        );
    }

    byte[] buildBinaryYson(Function<YsonConsumer, Void> builder) {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        YsonBinaryWriter consumer = new YsonBinaryWriter(output, 1);
        builder.apply(consumer);
        consumer.close();
        return output.toByteArray();
    }
}
