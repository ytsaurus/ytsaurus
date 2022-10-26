package tech.ytsaurus.yson;

import java.nio.charset.StandardCharsets;

/**
 * Interface for handling events emitted by yson parser.
 */
public interface YsonConsumer {
    /**
     * Called when entity (`#`) is encountered.
     */
    void onEntity();

    /**
     * Called when signed integer is encountered.
     */
    void onInteger(long value);

    /**
     * Called when unsigned integer is encountered.
     *
     * @param value encoded in the same way as `Long.parseUnsignedLong' does.
     */
    void onUnsignedInteger(long value);

    /**
     * Called when boolean is encountered.
     */
    void onBoolean(boolean value);

    /**
     * Called when double is encountered.
     */
    void onDouble(double value);

    /**
     * Called when string is encountered.
     *
     * Argument is byte array since yson string is an arbitrary byte sequence.
     */
    void onString(byte[] value, int offset, int length);

    /**
     * Convenience method equivalent to calling {@link #onString(byte[], int, int)} with utf-8 representation of java string.
     *
     * This is convenience method only.
     * Implementations shouldn't override this method or expect that it would be called.
     * Many users of YsonConsumer interface call {@link #onString(byte[], int, int)} as it's more generic.
     */
    default void onString(String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        onString(bytes, 0, bytes.length);
    }

    /**
     * Called when list begin is encountered.
     */
    void onBeginList();

    /**
     * Called before each list item.
     */
    void onListItem();

    /**
     * Called when list end is encountered.
     */
    void onEndList();

    /**
     * Called when attributes begin is encountered.
     */
    void onBeginAttributes();

    /**
     * Called when attributes begin is encountered.
     */
    void onEndAttributes();

    /**
     * Called when map begin is encountered.
     */
    void onBeginMap();

    /**
     * Called when map end is encountered.
     */
    void onEndMap();

    /**
     * Called when map key or attribute key is encountered.
     *
     * Argument is byte array since yson map key is an arbitrary byte sequence.
     */
    void onKeyedItem(byte[] value, int offset, int length);

    /**
     * Convenience method equivalent to calling {@link #onKeyedItem(byte[], int, int)} with utf-8 representation of java string.
     *
     * This is convenience method only.
     * Implementations shouldn't override this method or expect that it would be called.
     * Many users of YsonConsumer interface call {@link #onKeyedItem(byte[], int, int)} as it's more generic.
     */
    default void onKeyedItem(String key) {
        byte[] bytes = key.getBytes(StandardCharsets.UTF_8);
        onKeyedItem(bytes, 0, bytes.length);
    }

    static void onKeyedItem(YsonConsumer ysonConsumer, byte[] value) {
        ysonConsumer.onKeyedItem(value, 0, value.length);
    }

    static void onString(YsonConsumer ysonConsumer, byte[] value) {
        ysonConsumer.onString(value, 0, value.length);
    }
}
