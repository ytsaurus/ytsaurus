package tech.ytsaurus.client.request;

import com.google.protobuf.ByteString;

/**
 * A distributed write cookie that identifies a target distributed write partition and must be used in
 * WriteTableFragment method.
 */
public class DistributedWriteCookie extends BinaryEntity {
    public DistributedWriteCookie(ByteString payload) {
        super(payload);
    }
}
