package ru.yandex.yt.ytclient.ytree;

import java.util.Map;

public abstract class YTreeIntNode extends YTreeNode {
    private final long value;

    protected YTreeIntNode(long value, Map<String, YTreeNode> attributes) {
        super(attributes);
        this.value = value;
    }

    @Override
    public long longValue() {
        return value;
    }

    @Override
    public int intValue() {
        if (isSigned()) {
            if (value < -0x80000000L || value > 0x7fffffffL) {
                throw new IllegalStateException("Value is out of range for signed int");
            }
        } else {
            if (value < 0 || value > 0xffffffffL) {
                throw new IllegalStateException("Value is out of range for unsigned int");
            }
        }
        return (int) value;
    }

    public abstract boolean isSigned();

    @Override
    protected void writeValueTo(YTreeConsumer consumer) {
        if (isSigned()) {
            consumer.onInt64Scalar(value);
        } else {
            consumer.onUint64Scalar(value);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof YTreeIntNode)) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }

        YTreeIntNode that = (YTreeIntNode) o;

        return value == that.value && (value >= 0 || isSigned() == that.isSigned());
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (int) (value ^ (value >>> 32));
        return result;
    }
}
