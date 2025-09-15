package tech.ytsaurus.client.request;

import com.google.protobuf.ByteString;

/**
 * Descriptor containing a specification of readable data and a signature identifying that specification.
 * Corresponds to TTablePartitionCookie.
 */
public class TablePartitionCookie extends BinaryEntity {
    public TablePartitionCookie(ByteString payload) {
        super(payload);
    }
}
