package tech.ytsaurus.client.request;

import com.google.protobuf.ByteString;

/**
 * A distributed write session that identifies a whole distributed write process.
 */
public class DistributedWriteSession extends BinaryEntity {
    public DistributedWriteSession(ByteString payload) {
        super(payload);
    }
}
