package tech.ytsaurus.client;

import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;
import java.util.List;

import tech.ytsaurus.ysontree.YTreeBinarySerializer;
import tech.ytsaurus.ysontree.YTreeNode;

class TableAttachmentYsonReader extends TableAttachmentRowsetReader<YTreeNode> {
    @Override
    protected List<YTreeNode> parseMergedRow(ByteBuffer bb, int size) {
        byte[] data = new byte[size];
        bb.get(data);
        ByteArrayInputStream input = new ByteArrayInputStream(data);

        return YTreeBinarySerializer.deserializeAll(input);
    }
}
