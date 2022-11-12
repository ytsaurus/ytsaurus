package tech.ytsaurus.ysontree;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Utility class containing helpers for working with YTreeNode
 */
public final class YTree {

    private YTree() {
    }

    /**
     * Clones specified node.
     */
    public static YTreeNode deepCopy(@Nonnull YTreeNode node) {
        return YTree.builder().value(node).build();
    }

    /**
     * Create new {@link YTreeBuilder}
     */
    public static YTreeBuilder builder() {
        return new YTreeBuilder();
    }

    public static YTreeBuilder mapBuilder() {
        return builder().beginMap();
    }

    public static YTreeBuilder listBuilder() {
        return builder().beginList();
    }

    public static YTreeBuilder attributesBuilder() {
        return builder().beginAttributes();
    }

    public static YTreeNode node(@Nullable Object value) {
        return YTree.builder().value(value).build();
    }

    public static YTreeBooleanNode booleanNode(boolean value) {
        return new YTreeBooleanNodeImpl(value, null);
    }

    public static YTreeDoubleNode doubleNode(double value) {
        return new YTreeDoubleNodeImpl(value, null);
    }

    public static YTreeIntegerNode integerNode(long value) {
        return new YTreeIntegerNodeImpl(true, value, null);
    }

    public static YTreeIntegerNode unsignedIntegerNode(long value) {
        return new YTreeIntegerNodeImpl(false, value, null);
    }

    public static YTreeIntegerNode longNode(long value) {
        return new YTreeIntegerNodeImpl(true, value, null);
    }

    public static YTreeIntegerNode unsignedLongNode(long value) {
        return new YTreeIntegerNodeImpl(false, value, null);
    }

    public static YTreeStringNode stringNode(@Nonnull String value) {
        return new YTreeStringNodeImpl(value, null);
    }

    public static YTreeStringNode bytesNode(@Nonnull byte[] value) {
        return new YTreeStringNodeImpl(value, null);
    }

    public static YTreeEntityNode entityNode() {
        return new YTreeEntityNodeImpl(null);
    }

    public static YTreeEntityNode nullNode() {
        return entityNode();
    }

    public static YTreeNode booleanOrNullNode(@Nullable Boolean value) {
        return value == null ? nullNode() : booleanNode(value);
    }

    public static YTreeNode doubleOrNullNode(@Nullable Double value) {
        return value == null ? nullNode() : doubleNode(value);
    }

    public static YTreeNode integerOrNullNode(@Nullable Integer value) {
        return value == null ? nullNode() : integerNode(value);
    }

    public static YTreeNode unsignedIntegerOrNullNode(@Nullable Integer value) {
        return value == null ? nullNode() : unsignedIntegerNode(value);
    }

    public static YTreeNode longOrNullNode(@Nullable Long value) {
        return value == null ? nullNode() : longNode(value);
    }

    public static YTreeNode unsignedLongOrNullNode(@Nullable Long value) {
        return value == null ? nullNode() : unsignedLongNode(value);
    }

    public static YTreeNode stringOrNullNode(@Nullable String value) {
        return value == null ? nullNode() : stringNode(value);
    }

    public static YTreeNode bytesOrNullNode(@Nullable byte[] value) {
        return value == null ? nullNode() : bytesNode(value);
    }
}
