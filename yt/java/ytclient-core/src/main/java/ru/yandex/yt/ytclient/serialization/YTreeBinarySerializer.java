package ru.yandex.yt.ytclient.serialization;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

import tech.ytsaurus.yson.ClosableYsonConsumer;
import tech.ytsaurus.yson.YsonBinaryWriter;
import tech.ytsaurus.yson.YsonParser;

import ru.yandex.inside.yt.kosher.impl.ytree.YTreeNodeUtils;
import ru.yandex.inside.yt.kosher.impl.ytree.builder.YTree;
import ru.yandex.inside.yt.kosher.impl.ytree.builder.YTreeBuilder;
import ru.yandex.inside.yt.kosher.impl.ytree.object.YTreeSerializer;
import ru.yandex.inside.yt.kosher.operations.OperationContext;
import ru.yandex.inside.yt.kosher.operations.Yield;
import ru.yandex.inside.yt.kosher.tables.CloseableIterator;
import ru.yandex.inside.yt.kosher.ytree.YTreeEntityNode;
import ru.yandex.inside.yt.kosher.ytree.YTreeNode;

public class YTreeBinarySerializer extends ru.yandex.inside.yt.kosher.impl.ytree.YTreeBinarySerializer {
    public static <T> void serializeAllObjects(List<T> objects, YTreeSerializer<T> serializer, OutputStream output) {
        try (ClosableYsonConsumer writer = getSerializer(output)) {
            for (T obj : objects) {
                writer.onListItem();
                serializer.serialize(obj, writer);
            }
            writer.onListItem();
        }
    }

    public static CloseableIterator<YTreeNode> iterator(InputStream input, OperationContext context) {
        YTreeBuilder builder = new YTreeBuilder();
        YsonParser parser = new YsonParser(input, (1 << 16));
        return new CloseableIterator<YTreeNode>() {

            boolean hasNextChecked = false;
            YTreeNode next = null;
            int tableIndex = 0;
            long rowIndex = 0; // will either be set later from YTreeEntityNode or start from zero

            @Override
            public boolean hasNext() {
                hasNextChecked = true;
                if (next != null) {
                    return true;
                }
                for (; ; ) {
                    boolean parsed = parser.parseListFragmentItem(builder);
                    if (!parsed) {
                        return false;
                    }
                    YTreeNode node = builder.build();
                    if (node instanceof YTreeEntityNode) {
                        if (node.containsAttribute("table_index")) {
                            tableIndex = node.getAttributeOrThrow("table_index").intValue();
                            context.setTableIndex(tableIndex);
                        }
                        if (node.containsAttribute("row_index")) {
                            rowIndex = node.getAttributeOrThrow("row_index").longValue();
                        }
                        continue;
                    }
                    context.setRowIndex(rowIndex);
                    if (context.isSettingTableIndex()) {
                        node.putAttribute("table_index", YTree.integerNode(tableIndex));
                    }
                    if (context.isSettingRowIndex()) {
                        node.putAttribute("row_index", YTree.integerNode(rowIndex));
                    }
                    next = node;
                    return true;
                }
            }

            @Override
            public YTreeNode next() {
                if (!hasNextChecked && !hasNext()) {
                    throw new IllegalStateException();
                }
                rowIndex++;
                YTreeNode ret = next;
                next = null;
                hasNextChecked = false;
                return ret;
            }

            @Override
            public void close() throws Exception {
                input.close();
            }
        };
    }

    public static Yield<YTreeNode> yield(OutputStream[] output) {
        YsonBinaryWriter[] writers = new YsonBinaryWriter[output.length];
        for (int i = 0; i < output.length; ++i) {
            writers[i] = new YsonBinaryWriter(output[i], (1 << 16));
        }
        return new Yield<YTreeNode>() {

            @Override
            public void yield(int index, YTreeNode value) {
                writers[index].onListItem();
                YTreeNodeUtils.walk(value, writers[index], true);
            }

            @Override
            public void close() throws IOException {
                for (int i = 0; i < output.length; ++i) {
                    writers[i].close();
                    output[i].close();
                }
            }

        };
    }
}
