package ru.yandex.yt.ytclient.ytree;

import java.util.Collections;
import java.util.Map;

public class YTreeDoubleNode extends YTreeNode {
    private final double value;

    public YTreeDoubleNode(double value) {
        this(value, Collections.emptyMap());
    }

    public YTreeDoubleNode(double value, Map<String, YTreeNode> attributes) {
        super(attributes);
        this.value = value;
    }

    @Override
    public YTreeNodeType getType() {
        return YTreeNodeType.DOUBLE;
    }

    @Override
    public double doubleValue() {
        return value;
    }

    @Override
    protected void writeValueTo(YTreeConsumer consumer) {
        consumer.onDoubleScalar(value);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof YTreeDoubleNode)) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }

        YTreeDoubleNode that = (YTreeDoubleNode) o;

        return Double.compare(that.value, value) == 0;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        long temp;
        temp = Double.doubleToLongBits(value);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        return result;
    }
}
