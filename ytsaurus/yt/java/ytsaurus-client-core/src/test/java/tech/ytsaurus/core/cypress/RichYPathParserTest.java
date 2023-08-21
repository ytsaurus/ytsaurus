package tech.ytsaurus.core.cypress;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.junit.Assert;
import org.junit.Test;
import tech.ytsaurus.ysontree.YTree;

public class RichYPathParserTest {
    @Test
    public void testYsonStringRepresentation() throws Exception {
        var reader = new BufferedReader(new InputStreamReader(
                Objects.requireNonNull(getClass().getClassLoader().getResourceAsStream("good-rich-ypath.txt")),
                StandardCharsets.UTF_8));

        // Compare yson string representation of parsed RichYPath
        while (true) {
            var line = reader.readLine();
            if (line == null) {
                break;
            }
            if (line.startsWith("##")) {
                continue;
            }
            if (line.isEmpty()) {
                continue;
            }
            if (line.startsWith("===")) {
                StringBuilder binaryYPathSb = new StringBuilder();
                line = reader.readLine();
                while (!line.startsWith("---")) {
                    binaryYPathSb.append(line);
                    binaryYPathSb.append("\n");
                    line = reader.readLine();
                }

                StringBuilder textYsonSb = new StringBuilder();
                line = reader.readLine();
                while (!line.startsWith("===")) {
                    textYsonSb.append(line);
                    textYsonSb.append("\n");
                    line = reader.readLine();
                }

                var binaryYPath = binaryYPathSb.toString().strip();
                var textYson = textYsonSb.toString().strip();

                Assert.assertEquals(
                        RichYPathParser.parse(binaryYPath).toStableString(), textYson);
            }
        }
    }

    @Test
    public void testExceptions() throws Exception {
        var reader = new BufferedReader(new InputStreamReader(
                Objects.requireNonNull(getClass().getClassLoader().getResourceAsStream("bad-rich-ypath.txt")),
                StandardCharsets.UTF_8));

        while (true) {
            var line = reader.readLine();
            if (line == null) {
                break;
            }
            if (line.startsWith("##")) {
                continue;
            }
            if (line.isEmpty()) {
                continue;
            }

            if (line.startsWith("===")) {
                StringBuilder binaryYPathSb = new StringBuilder();
                line = reader.readLine();
                while (!line.startsWith("===")) {
                    binaryYPathSb.append(line);
                    binaryYPathSb.append("\n");
                    line = reader.readLine();
                }
                var binaryYPath = binaryYPathSb.toString().strip();

                Assert.assertThrows(
                        "Path '" + binaryYPath + "' shouldn't parse correctly",
                        Exception.class, () -> {
                            RichYPathParser.parse(binaryYPath);
                        });
            }
        }
    }

    @Test
    public void testRichYPath() {
        Assert.assertEquals(
                RichYPathParser.parse("<a=b>//home/ignat{a,b}[100:200]"),
                RichYPath.simple("//home/ignat")
                        .withRange(RangeLimit.key(YTree.integerNode(100)), RangeLimit.key(YTree.integerNode(200)))
                        .withColumns(List.of("a", "b"))
                        .withAdditionalAttributes(Map.of("a", YTree.stringNode("b")))
        );

        Assert.assertEquals(
                RichYPathParser.parse("<a=b>//home"),
                RichYPath.simple("//home")
                        .withAdditionalAttributes(Map.of("a", YTree.stringNode("b")))
        );

        Assert.assertEquals(
                RichYPathParser.parse("//home"),
                RichYPath.simple("//home")
        );

        Assert.assertEquals(
                RichYPathParser.parse("//home[:]"),
                RichYPath.simple("//home").plusRange(Range.builder().build())
        );

        Assert.assertEquals(
                RichYPathParser.parse("//home[(x, y):(a, b)]"),
                RichYPath.simple("//home").withRange(
                        RangeLimit.key(YTree.stringNode("x"), YTree.stringNode("y")),
                        RangeLimit.key(YTree.stringNode("a"), YTree.stringNode("b"))));

        Assert.assertEquals(
                RichYPathParser.parse("//home[#1:#2,x:y]"),
                RichYPath.simple("//home")
                        .withRange(RangeLimit.row(1), RangeLimit.row(2))
                        .withRange(
                                RangeLimit.key(YTree.stringNode("x")),
                                RangeLimit.key(YTree.stringNode("y"))));

        Assert.assertEquals(RichYPathParser.parse("//home[x:#1000]"),
                RichYPath.simple("//home").withRange(RangeLimit.key(YTree.stringNode("x")), RangeLimit.row(1000)));

        Assert.assertEquals(RichYPathParser.parse(" <a=b> //home"),
                RichYPath.simple("//home").withAdditionalAttributes(Map.of("a", YTree.stringNode("b"))));

    }
}
