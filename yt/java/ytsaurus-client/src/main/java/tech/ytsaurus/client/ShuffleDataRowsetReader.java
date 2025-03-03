package tech.ytsaurus.client;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;

import javax.annotation.Nullable;

import tech.ytsaurus.client.rows.UnversionedRow;
import tech.ytsaurus.client.rows.UnversionedRowDeserializer;

public class ShuffleDataRowsetReader extends TableAttachmentWireProtocolReader<UnversionedRow> {
    ShuffleDataRowsetReader() {
        super(new UnversionedRowDeserializer());
    }

    @Override
    public List<UnversionedRow> parse(@Nullable byte[] attachments) throws Exception {
        if (attachments == null) {
            return null;
        }
        ByteBuffer bb = ByteBuffer.wrap(attachments).order(ByteOrder.LITTLE_ENDIAN);
        return parseRowData(bb, attachments.length);
    }
}
