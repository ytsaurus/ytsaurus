package tech.ytsaurus.client;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;

import javax.annotation.Nullable;

public class TableAttachmentByteBufferPartitionReader extends TableAttachmentByteBufferReader {
    @Override
    public List<ByteBuffer> parse(@Nullable byte[] attachments) throws Exception {
        if (attachments == null) {
            return null;
        }
        ByteBuffer bb = ByteBuffer.wrap(attachments).order(ByteOrder.LITTLE_ENDIAN);
        return parseRowData(bb, attachments.length);
    }
}
