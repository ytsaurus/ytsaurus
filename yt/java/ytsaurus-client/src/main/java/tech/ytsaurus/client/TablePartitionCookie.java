package tech.ytsaurus.client;

import com.google.protobuf.ByteString;

public class TablePartitionCookie {
    // Descriptor containing a specification of readable data and a signature identifying that specification.
    // Corresponds to TTablePartitionCookie.
    private final ByteString payload;

    public TablePartitionCookie(ByteString payload) {
        this.payload = payload;
    }

    public ByteString getPayload() {
        return payload;
    }
}
