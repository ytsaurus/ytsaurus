package ru.yandex.yt.ytclient.ytree;

import java.util.Collections;
import java.util.Map;

public class YTreeBooleanNode extends YTreeNode {
    public static final YTreeBooleanNode TRUE = new YTreeBooleanNode(true);
    public static final YTreeBooleanNode FALSE = new YTreeBooleanNode(false);

    private final boolean value;

    public YTreeBooleanNode(boolean value) {
        this(value, Collections.emptyMap());
    }

    public YTreeBooleanNode(boolean value, Map<String, YTreeNode> attributes) {
        super(attributes);
        this.value = value;
    }

    @Override
    public YTreeNodeType getType() {
        return YTreeNodeType.BOOLEAN;
    }

    @Override
    public boolean booleanValue() {
        return value;
    }

    @Override
    protected void writeValueTo(YTreeConsumer consumer) {
        consumer.onBooleanScalar(value);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof YTreeBooleanNode)) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }

        YTreeBooleanNode that = (YTreeBooleanNode) o;

        return value == that.value;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (value ? 1 : 0);
        return result;
    }
}
