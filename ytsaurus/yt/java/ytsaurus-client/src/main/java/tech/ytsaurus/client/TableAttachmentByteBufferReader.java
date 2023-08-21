package tech.ytsaurus.client;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

class TableAttachmentByteBufferReader extends TableAttachmentRowsetReader<ByteBuffer> {
    @Override
    protected List<ByteBuffer> parseMergedRow(ByteBuffer bb, int size) {
        ByteBuffer res = bb.duplicate();
        res.position(bb.position());
        res.limit(bb.position() + size);
        bb.position(bb.position() + size);
        return Collections.singletonList(res);
    }
}
