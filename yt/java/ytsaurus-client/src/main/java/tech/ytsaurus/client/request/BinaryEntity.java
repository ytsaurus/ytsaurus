package tech.ytsaurus.client.request;

import java.io.Serializable;

import com.google.protobuf.ByteString;

abstract class BinaryEntity implements Serializable {
    private final ByteString payload;

    BinaryEntity(ByteString payload) {
        this.payload = payload;
    }

    public ByteString getPayload() {
        return payload;
    }
}
