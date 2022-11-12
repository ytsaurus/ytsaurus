package tech.ytsaurus.ysontree;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

import tech.ytsaurus.yson.ClosableYsonConsumer;
import tech.ytsaurus.yson.YsonBinaryWriter;
import tech.ytsaurus.yson.YsonConsumer;
import tech.ytsaurus.yson.YsonParser;

public class YTreeBinarySerializer {
    public static final int DEFAULT_BUFFER_SIZE = 4096;

    protected YTreeBinarySerializer() {
    }

    public static ClosableYsonConsumer getSerializer(OutputStream output) {
        return getSerializer(output, DEFAULT_BUFFER_SIZE);
    }

    public static ClosableYsonConsumer getSerializer(OutputStream output, int bufferSize) {
        // N.B. for historical reasons we override close here so it doesn't close output.
        return new YsonBinaryWriter(output, bufferSize) {
            @Override
            public void close() {
                super.flush();
            }
        };
    }

    public static void serialize(YTreeNode node, OutputStream output) {
        ClosableYsonConsumer writer = getSerializer(output);
        YTreeNodeUtils.walk(node, writer, true);
        writer.close();
    }

    public static InputStream serializeAndGet(YTreeNode node) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        serialize(node, baos);
        return new ByteArrayInputStream(baos.toByteArray());
    }

    /*
     * This method will consume whole input stream and will throw {@link YsonError} exception if
     * stream contains trailing (non whitespace) bytes.
     */
    public static YTreeNode deserialize(InputStream input) {
        return deserialize(input, new byte[DEFAULT_BUFFER_SIZE]);
    }

    /*
     * This method will consume whole input stream and will throw {@link YsonError} exception if
     * stream contains trailing (non whitespace) bytes.
     */
    public static YTreeNode deserialize(InputStream input, byte[] buffer) {
        YTreeBuilder builder = YTree.builder();
        YsonParser parser = new YsonParser(input, buffer);
        parser.parseNode(builder);
        return builder.build();
    }

    /*
     * This method will consume whole input stream and will throw {@link YsonError} exception if
     * stream contains trailing (non whitespace) bytes.
     */
    public static void deserialize(InputStream input, YsonConsumer consumer) {
        YsonParser parser = new YsonParser(input, DEFAULT_BUFFER_SIZE);
        parser.parseNode(consumer);
    }

    /*
     * This method will read all `yson` items from stream .
     */
    public static List<YTreeNode> deserializeAll(InputStream input) {
        return deserializeAll(input, new byte[DEFAULT_BUFFER_SIZE]);
    }

    /*
     * This method will read all `yson` items from stream.
     */
    public static List<YTreeNode> deserializeAll(InputStream input, byte[] buffer) {
        YTreeBuilder builder = YTree.builder().beginList();
        YsonParser parser = new YsonParser(input, buffer);
        while (parser.parseListFragmentItem(builder)) {
            continue;
        }
        return builder.buildList().asList();
    }
}
