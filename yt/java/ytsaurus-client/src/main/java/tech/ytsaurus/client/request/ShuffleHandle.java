package tech.ytsaurus.client.request;

import java.io.Serializable;

import com.google.protobuf.ByteString;

public class ShuffleHandle implements Serializable {
    private final ByteString payload;

    public ShuffleHandle(ByteString payload) {
        this.payload = payload;
    }

    public ByteString getPayload() {
        return payload;
    }
}
