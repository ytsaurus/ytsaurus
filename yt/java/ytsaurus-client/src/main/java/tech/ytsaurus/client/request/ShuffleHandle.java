package tech.ytsaurus.client.request;

import com.google.protobuf.ByteString;

public class ShuffleHandle extends BinaryEntity {
    public ShuffleHandle(ByteString payload) {
        super(payload);
    }
}
