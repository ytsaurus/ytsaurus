package tech.ytsaurus.ysontree;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import tech.ytsaurus.yson.ClosableYsonConsumer;
import tech.ytsaurus.yson.YsonParser;
import tech.ytsaurus.yson.YsonTextWriter;

public final class YTreeTextSerializer {
    private YTreeTextSerializer() {
    }

    /**
     * Serializes a {@link YTreeNode} into its YSON text representation.
     *
     * @param node the {@link YTreeNode} to serialize.
     * @return the YSON text representation of the node.
     */
    public static String serialize(YTreeNode node) {
        StringBuilder sb = new StringBuilder();
        try (ClosableYsonConsumer ysonConsumer = new YsonTextWriter(sb)) {
            YTreeNodeUtils.walk(node, ysonConsumer, false);
        }
        return sb.toString();
    }

    /**
     * Serializes a {@link YTreeNode} into a pretty-printed YSON string representation.
     *
     * @param node the {@link YTreeNode} to serialize
     * @return the pretty-printed YSON string representation of the node.
     */
    public static String serializePretty(YTreeNode node) {
        StringBuilder sb = new StringBuilder();
        try (ClosableYsonConsumer ysonConsumer = YsonTextWriter.builder()
                .setStringBuilder(sb)
                .setPrettyPrinting()
                .build()
        ) {
            YTreeNodeUtils.walk(node, ysonConsumer, false);
        }
        return sb.toString();
    }

    /**
     * Serializes a {@link YTreeNode} into a stable YSON text representation.
     * {@code node} attributes are sorted by their keys before serialization.
     *
     * @param node the {@link YTreeNode} to serialize.
     * @return the stable YSON text representation of the given node.
     */
    public static String stableSerialize(YTreeNode node) {
        StringBuilder sb = new StringBuilder();
        try (ClosableYsonConsumer ysonConsumer = new YsonTextWriter(sb)) {
            YTreeNodeUtils.walk(node, ysonConsumer, false, true);
        }
        return sb.toString();
    }

    /**
     * Serializes a {@link YTreeNode} into a stable pretty-printed YSON text representation.
     * {@code node} attributes are sorted by their keys before serialization.
     *
     * @param node the {@link YTreeNode} to serialize.
     * @return the stable pretty-printed YSON text representation of the given node.
     */
    public static String stableSerializePretty(YTreeNode node) {
        StringBuilder sb = new StringBuilder();
        try (ClosableYsonConsumer ysonConsumer = YsonTextWriter.builder()
                .setStringBuilder(sb)
                .setPrettyPrinting()
                .build()
        ) {
            YTreeNodeUtils.walk(node, ysonConsumer, false, true);
        }
        return sb.toString();
    }

    /**
     * Deserializes a YSON node from the specified {@link InputStream}.
     *
     * @param input the input stream containing YSON data.
     * @return the deserialized {@link YTreeNode}.
     * @throws java.io.UncheckedIOException if an I/O error occurs.
     */
    public static YTreeNode deserialize(InputStream input) {
        YTreeBuilder builder = YTree.builder();
        YsonParser ysonParser = new YsonParser(input);
        ysonParser.parseNode(builder);
        return builder.build();
    }

    /**
     * Deserializes a YSON node from the specified String.
     *
     * @param text the input String containing YSON data.
     * @return the deserialized {@link YTreeNode}.
     * @throws java.io.UncheckedIOException if an I/O error occurs.
     */
    public static YTreeNode deserialize(String text) {
        return deserialize(new ByteArrayInputStream(text.getBytes(StandardCharsets.UTF_8)));
    }
}
