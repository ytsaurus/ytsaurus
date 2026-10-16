package tech.ytsaurus.core.rows;

import tech.ytsaurus.typeinfo.TiType;
import tech.ytsaurus.yson.YsonConsumer;
import tech.ytsaurus.ysontree.YTreeEntityNode;
import tech.ytsaurus.ysontree.YTreeNode;

/**
 * Serializer for byte arrays represented as YSON strings.
 */
public class YTreeBytesSerializer implements YTreeSerializer<byte[]> {
    @Override
    public void serialize(byte[] bytes, YsonConsumer consumer) {
        consumer.onString(bytes, 0, bytes.length);
    }

    @Override
    public byte[] deserialize(YTreeNode node) {
        if (node instanceof YTreeEntityNode) {
            return null;
        }
        return node.bytesValue();
    }

    @Override
    public TiType getColumnValueType() {
        return TiType.optional(TiType.yson());
    }

}
