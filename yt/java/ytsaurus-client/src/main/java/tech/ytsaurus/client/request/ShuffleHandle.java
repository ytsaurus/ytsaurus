package tech.ytsaurus.client.request;

import java.io.Serializable;

import javax.annotation.Nullable;

import com.google.protobuf.ByteString;
import tech.ytsaurus.ysontree.YTreeBinarySerializer;
import tech.ytsaurus.ysontree.YTreeNode;

public class ShuffleHandle implements Serializable {
    private final ByteString payload;
    @Nullable
    private transient YTreeNode yTreeNode = null;

    public ShuffleHandle(ByteString payload) {
        this.payload = payload;
    }

    public ByteString getPayload() {
        return payload;
    }

    public YTreeNode asYTreeNode() {
        if (yTreeNode == null) {
            yTreeNode = YTreeBinarySerializer.deserialize(payload.newInput());
        }
        return yTreeNode;
    }
}
