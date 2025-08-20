package tech.ytsaurus.client;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;

import javax.annotation.Nullable;

import tech.ytsaurus.client.rows.EntitySkiffSerializer;

public class TableAttachmentSkiffPartitionReader<T> extends TableAttachmentSkiffReader<T> {
    public TableAttachmentSkiffPartitionReader(EntitySkiffSerializer<T> serializer) {
        super(serializer);
    }

    @Override
    public List<T> parse(@Nullable byte[] attachments) throws Exception {
        if (attachments == null) {
            return null;
        }
        ByteBuffer bb = ByteBuffer.wrap(attachments).order(ByteOrder.LITTLE_ENDIAN);
        return parseRowData(bb, attachments.length);
    }
}
