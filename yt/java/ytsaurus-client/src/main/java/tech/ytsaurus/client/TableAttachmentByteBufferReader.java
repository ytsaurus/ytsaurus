package tech.ytsaurus.client;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

import javax.annotation.Nullable;

class TableAttachmentByteBufferReader extends TableAttachmentRowsetReader<ByteBuffer> {
    private static final int END_OF_STREAM_MARKER_SIZE = Long.BYTES;

    private volatile boolean endOfStream;

    @Override
    public boolean isEndOfStream() {
        return endOfStream;
    }

    @Override
    public List<ByteBuffer> endOfStream() {
        if (endOfStream) {
            return null;
        }
        endOfStream = true;
        return Collections.singletonList(ByteBuffer.wrap(new byte[END_OF_STREAM_MARKER_SIZE]));
    }

    @Override
    public List<ByteBuffer> parse(@Nullable byte[] attachment) throws Exception {
        return parse(attachment, 0, attachment == null ? 0 : attachment.length);
    }

    @Override
    public List<ByteBuffer> parse(@Nullable byte[] attachment, int offset, int length) throws Exception {
        if (attachment == null) {
            return null;
        }
        return super.parse(attachment, offset, length);
    }

    @Override
    protected List<ByteBuffer> parseMergedRow(ByteBuffer bb, int size) {
        ByteBuffer res = bb.duplicate();
        res.limit(bb.position() + size);
        bb.position(bb.position() + size);
        return Collections.singletonList(res);
    }
}
