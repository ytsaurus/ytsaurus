package tech.ytsaurus.ysontree;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class YTreeStringNodeImpl extends YTreeNodeImpl implements YTreeStringNode {

    private byte[] bytes;
    private String value;

    public YTreeStringNodeImpl(String value, @Nullable Map<String, YTreeNode> attributes) {
        super(attributes);
        this.value = value;
    }

    public YTreeStringNodeImpl(byte[] bytes, @Nullable Map<String, YTreeNode> attributes) {
        super(attributes);
        this.bytes = bytes;
    }

    @Override
    public String getValue() {
        if (value == null && bytes != null) {
            value = new String(bytes, StandardCharsets.UTF_8);
        }
        return value;
    }

    @Override
    public String setValue(String newValue) {
        String ret = getValue();
        value = newValue;
        bytes = null;
        return ret;
    }

    @Override
    public byte[] getBytes() {
        if (bytes == null && value != null) {
            bytes = value.getBytes(StandardCharsets.UTF_8);
        }
        return bytes;
    }

    @Override
    public byte[] setBytes(byte[] newBytes) {
        byte[] ret = getBytes();
        bytes = newBytes;
        value = null;
        return ret;
    }

    @Nonnull
    @Override
    public String getBoxedValue() {
        String val = getValue();
        return val != null ? val : "";
    }

    @Override
    public int hashCode() {
        return hashCodeBase() * 4243 + Arrays.hashCode(getBytes());
    }

    @Override
    public boolean equals(Object another) {
        if (this == another) {
            return true;
        }
        if (another == null || !(another instanceof YTreeStringNode)) {
            return false;
        }
        YTreeStringNode node = (YTreeStringNode) another;
        return Arrays.equals(getBytes(), node.getBytes()) && equalsBase(node);
    }

}
