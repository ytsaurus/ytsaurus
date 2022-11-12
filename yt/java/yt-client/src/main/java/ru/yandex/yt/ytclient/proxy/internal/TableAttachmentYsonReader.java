package ru.yandex.yt.ytclient.proxy.internal;

import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;
import java.util.List;

import tech.ytsaurus.ysontree.YTreeBinarySerializer;
import tech.ytsaurus.ysontree.YTreeNode;


public class TableAttachmentYsonReader extends TableAttachmentRowsetReader<YTreeNode> {
    public TableAttachmentYsonReader() {
    }

    @Override
    protected List<YTreeNode> parseMergedRow(ByteBuffer bb, int size) {
        byte[] data = new byte[size];
        bb.get(data);
        ByteArrayInputStream input = new ByteArrayInputStream(data);

        return YTreeBinarySerializer.deserializeAll(input);
    }
}
