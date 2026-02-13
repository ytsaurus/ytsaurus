package tech.ytsaurus.core.cypress;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.junit.jupiter.api.Test;
import tech.ytsaurus.ysontree.YTree;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

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

                assertEquals(RichYPathParser.parse(binaryYPath).toStableString(), textYson);
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

                assertThrows(
                        Exception.class,
                        () -> RichYPathParser.parse(binaryYPath),
                        "Path '" + binaryYPath + "' shouldn't parse correctly"
                );
            }
        }
    }

    @Test
    public void testRichYPath() {
        assertEquals(
                RichYPathParser.parse("<a=b>//home/ignat{a,b}[100:200]"),
                RichYPath.simple("//home/ignat")
                        .withRange(RangeLimit.key(YTree.integerNode(100)), RangeLimit.key(YTree.integerNode(200)))
                        .withColumns(List.of("a", "b"))
                        .withAdditionalAttributes(Map.of("a", YTree.stringNode("b")))
        );

        assertEquals(
                RichYPathParser.parse("<a=b>//home"),
                RichYPath.simple("//home")
                        .withAdditionalAttributes(Map.of("a", YTree.stringNode("b")))
        );

        assertEquals(
                RichYPathParser.parse("//home"),
                RichYPath.simple("//home")
        );

        assertEquals(
                RichYPathParser.parse("//home[:]"),
                RichYPath.simple("//home").plusRange(Range.builder().build())
        );

        assertEquals(
                RichYPathParser.parse("//home[(x, y):(a, b)]"),
                RichYPath.simple("//home").withRange(
                        RangeLimit.key(YTree.stringNode("x"), YTree.stringNode("y")),
                        RangeLimit.key(YTree.stringNode("a"), YTree.stringNode("b")))
        );

        assertEquals(
                RichYPathParser.parse("//home[#1:#2,x:y]"),
                RichYPath.simple("//home")
                        .withRange(RangeLimit.row(1), RangeLimit.row(2))
                        .withRange(
                                RangeLimit.key(YTree.stringNode("x")),
                                RangeLimit.key(YTree.stringNode("y")))
        );

        assertEquals(RichYPathParser.parse("//home[x:#1000]"),
                RichYPath.simple("//home").withRange(RangeLimit.key(YTree.stringNode("x")), RangeLimit.row(1000))
        );

        assertEquals(RichYPathParser.parse(" <a=b> //home"),
                RichYPath.simple("//home").withAdditionalAttributes(Map.of("a", YTree.stringNode("b")))
        );

        assertEquals(
                RichYPathParser.parse(
                        "<\"ranges\"=[{" +
                                "\"lower_limit\"={\"row_index\"=0;\"tablet_index\"=0;};" +
                                "\"upper_limit\"={\"row_index\"=1;\"tablet_index\"=0;};};];>" +
                                "//home"
                ),
                RichYPath.simple("//home")
                        .withRange(
                                RangeLimit.builder().setRowIndex(0).setTabletIndex(0).build(),
                                RangeLimit.builder().setRowIndex(1).setTabletIndex(0).build()
                        )
        );
    }
}
