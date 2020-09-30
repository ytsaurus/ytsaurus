package ru.yandex.yson;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import javax.annotation.Nonnull;

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
    void onString(@Nonnull byte[] value);

    default void onString(@Nonnull byte[] value, int offset, int length) {
        onString(Arrays.copyOfRange(value, offset, offset + length));
    }

    /**
     * Convenience method equivalent to calling {@link #onString(byte[])} with utf-8 representation of java string.
     *
     * This is convenience method only.
     * Implementations shouldn't override this method or expect that it would be called.
     * Many users of YsonConsumer interface call {@link #onString(byte[])} as it's more generic.
     */
    default void onString(@Nonnull String value) {
        onString(value.getBytes(StandardCharsets.UTF_8));
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
     */
    void onKeyedItem(@Nonnull byte[] value);

    default void onKeyedItem(@Nonnull byte[] value, int offset, int length) {
        onKeyedItem(Arrays.copyOfRange(value, offset, offset + length));
    }

    /**
     * Convenience method equivalent to calling {@link #onKeyedItem(byte[])} with utf-8 representation of java string.
     *
     * This is convenience method only.
     * Implementations shouldn't override this method or expect that it would be called.
     * Many users of YsonConsumer interface call {@link #onKeyedItem(byte[])} as it's more generic.
     */
    default void onKeyedItem(@Nonnull String key) {
        onKeyedItem(key.getBytes(StandardCharsets.UTF_8));
    }
}
