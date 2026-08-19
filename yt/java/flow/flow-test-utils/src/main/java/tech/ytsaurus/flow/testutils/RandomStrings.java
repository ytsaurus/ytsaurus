package tech.ytsaurus.flow.testutils;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Minimal helper for generating random alphanumeric strings for synthetic test data.
 */
public final class RandomStrings {
    private static final char[] ALPHANUMERIC =
            "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789".toCharArray();

    private RandomStrings() {
    }

    /**
     * Returns a random string of the given length consisting of characters {@code [a-zA-Z0-9]}.
     */
    public static String nextAlphanumeric(int length) {
        if (length < 0) {
            throw new IllegalArgumentException("Length must be non-negative: " + length);
        }
        var random = ThreadLocalRandom.current();
        var chars = new char[length];
        for (int i = 0; i < length; ++i) {
            chars[i] = ALPHANUMERIC[random.nextInt(ALPHANUMERIC.length)];
        }
        return new String(chars);
    }
}
