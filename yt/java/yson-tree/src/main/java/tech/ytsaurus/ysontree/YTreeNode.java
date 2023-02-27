package tech.ytsaurus.ysontree;

import java.nio.charset.Charset;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

/**
 * YTreeNode is representation of yson object.
 *
 * To build such object {@link tech.ytsaurus.ysontree.YTreeBuilder} can be used.
 *
 * @see tech.ytsaurus.ysontree.YTree
 * @see <a href="https://yt.yandex-team.ru/docs/description/common/yson">Yson documentation</a>
 */
public interface YTreeNode {
    /**
     * Get attribute map of an object.
     *
     * Return empty map if object doesn't have attributes.
     */
    Map<String, YTreeNode> getAttributes();

    void clearAttributes();

    boolean containsAttributes();

    boolean containsAttribute(String key);

    Optional<YTreeNode> removeAttribute(String key);

    Optional<YTreeNode> putAttribute(String key, YTreeNode value);

    default Set<String> attributeKeys() {
        return getAttributes().keySet();
    }

    default Collection<YTreeNode> attributeValues() {
        return getAttributes().values();
    }

    Optional<YTreeNode> getAttribute(String key);

    YTreeNode getAttributeOrThrow(String key);

    YTreeNode getAttributeOrThrow(String key, Supplier<String> createMessage);

    default <T extends YTreeNode> T cast() {
        return (T) this;
    }

    default YTreeListNode listNode() {
        return cast();
    }

    default YTreeMapNode mapNode() {
        return cast();
    }

    default YTreeBooleanNode booleanNode() {
        return cast();
    }

    default YTreeIntegerNode integerNode() {
        return cast();
    }

    default YTreeDoubleNode doubleNode() {
        return cast();
    }

    default YTreeStringNode stringNode() {
        return cast();
    }

    default YTreeEntityNode entityNode() {
        return cast();
    }

    default <V> YTreeScalarNode<V> scalarNode() {
        return cast();
    }

    /**
     * Get list value assuming node contains it.
     *
     * @throws RuntimeException if node is not integral.
     */
    default List<YTreeNode> asList() {
        throw new UnsupportedOperationException("asList() unsupported in " + getClass());
    }

    /**
     * Get map value assuming node contains it.
     *
     * @throws RuntimeException if node is not integral.
     */
    default Map<String, YTreeNode> asMap() {
        throw new UnsupportedOperationException("asMap() unsupported in " + getClass());
    }

    /**
     * Get integral value assuming node contains it.
     *
     * Can be used with nodes containing unsigned integral value in that case returned value should be interpreted
     * as bit representation of unsigned value and `Long.*Unsigned*` methods can be used to work with it.
     *
     * @throws RuntimeException if node is not integral.
     *
     * @see Long
     */
    default long longValue() {
        return this.<YTreeIntegerNode>cast().getLong();
    }

    /**
     * Shortcut for `(int) node.getLong()`
     *
     * @see #longValue()
     */
    default int intValue() {
        return (int) longValue();
    }

    /**
     * Get floating point value assuming node contains it.
     *
     * @throws RuntimeException if node is of unexpected type.
     */
    default double doubleValue() {
        return this.<YTreeDoubleNode>cast().getValue();
    }

    default float floatValue() {
        return (float) this.<YTreeDoubleNode>cast().getValue();
    }

    /**
     * Get boolean value assuming node contains it.
     *
     * @throws RuntimeException if node is of an unexpected type.
     */
    default boolean boolValue() {
        return this.<YTreeBooleanNode>cast().getValue();
    }

    /**
     * Get string value assuming node is string-like.
     *
     * If node contains binary data it would be interpreted as UTF8 string
     * like {@link String#String(byte[], Charset)} does.
     *
     * @throws RuntimeException if node is of an unexpected type.
     */
    default String stringValue() {
        return this.<YTreeStringNode>cast().getValue();
    }

    /**
     * Get bytes value assuming node is string-like.
     *
     * If node was constructed with {@link String} object
     * UTF8 representation of string is returned.
     *
     * @throws RuntimeException if node is of an unexpected type.
     */
    default byte[] bytesValue() {
        return this.<YTreeStringNode>cast().getBytes();
    }

    /**
     * Check if node contains list value.
     */
    default boolean isListNode() {
        return this instanceof YTreeListNode;
    }

    /**
     * Check if node contains map value.
     */
    default boolean isMapNode() {
        return this instanceof YTreeMapNode;
    }

    /**
     * Check if node contains boolean value.
     */
    default boolean isBooleanNode() {
        return this instanceof YTreeBooleanNode;
    }

    /**
     * Check if node contains signed or unsigned integral value.
     *
     * Note: value of integer node always fits Java's long type but not always fit Java's int type.
     */
    default boolean isIntegerNode() {
        return this instanceof YTreeIntegerNode;
    }

    /**
     * Check if node contains signed or unsigned integral value.
     *
     * Note, it doesn't mean that value fits Java's int type.
     */
    default boolean isDoubleNode() {
        return this instanceof YTreeDoubleNode;
    }

    /**
     * Check if node contains string-like value.
     *
     * Note, string-like value can contain arbitrary byte sequence.
     */
    default boolean isStringNode() {
        return this instanceof YTreeStringNode;
    }

    /**
     * Check if node contains null (or entity) value.
     */
    default boolean isEntityNode() {
        return this instanceof YTreeEntityNode;
    }

    /**
     * Get binary yson representation of value.
     *
     * @see tech.ytsaurus.yson.YsonBinaryWriter
     */
    byte[] toBinary();
}
