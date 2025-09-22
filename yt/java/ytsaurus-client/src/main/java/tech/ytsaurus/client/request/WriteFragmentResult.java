package tech.ytsaurus.client.request;

import com.google.protobuf.ByteString;

/**
 * A result of WriteTableFragment invocation that must be passed to FinishDistributedWriteSession.
 */
public class WriteFragmentResult extends BinaryEntity {
    public WriteFragmentResult(ByteString payload) {
        super(payload);
    }
}
