package ru.yandex.inside.yt.kosher.impl.ytree;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;

import ru.yandex.inside.yt.kosher.impl.ytree.builder.YTree;
import ru.yandex.inside.yt.kosher.impl.ytree.builder.YTreeBuilder;
import ru.yandex.inside.yt.kosher.ytree.YTreeNode;
import ru.yandex.yson.ClosableYsonConsumer;
import ru.yandex.yson.YsonBinaryWriter;
import ru.yandex.yson.YsonConsumer;
import ru.yandex.yson.YsonParser;

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

    public static YTreeNode deserialize(InputStream input) {
        return deserialize(input, new byte[DEFAULT_BUFFER_SIZE]);
    }

    public static YTreeNode deserialize(InputStream input, byte[] buffer) {
        YTreeBuilder builder = YTree.builder();
        YsonParser parser = new YsonParser(input, buffer);
        parser.parseNode(builder);
        return builder.build();
    }

    public static void deserialize(InputStream input, YsonConsumer consumer) {
        YsonParser parser = new YsonParser(input, DEFAULT_BUFFER_SIZE);
        parser.parseNode(consumer);
    }
}
