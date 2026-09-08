package tech.ytsaurus.flow.service;

import java.nio.charset.StandardCharsets;

/**
 * Wraps a throwable with a message that flattens its cause chain, bounded in depth
 * and UTF-8 length, so the message is safe to use as a gRPC status description.
 */
public class TruncatedException extends RuntimeException {

    // Bounds for the flattened cause chain; the description travels percent-encoded
    // in the gRPC status (up to three wire bytes per UTF-8 byte), so its UTF-8 size
    // must stay well below gRPC's default 8 KB metadata soft limit (the worker's
    // channel does not override GRPC_ARG_MAX_METADATA_SIZE).
    private static final int MAX_CAUSE_DEPTH = 10;
    private static final int MAX_MESSAGE_UTF8_BYTES = 2048;

    public TruncatedException(String prefix, Throwable cause) {
        super(buildMessage(prefix, cause), cause);
    }

    private static String buildMessage(String prefix, Throwable throwable) {
        StringBuilder message = new StringBuilder(prefix);
        message.append(": ").append(describe(throwable));
        Throwable cause = throwable.getCause();
        for (int depth = 0; cause != null && depth < MAX_CAUSE_DEPTH; depth++) {
            message.append("; caused by: ").append(describe(cause));
            cause = cause.getCause();
        }
        if (cause != null) {
            message.append("; ...");
        }
        return truncateUtf8(message.toString(), MAX_MESSAGE_UTF8_BYTES);
    }

    private static String describe(Throwable throwable) {
        try {
            return throwable.toString();
        } catch (Throwable e) {
            return throwable.getClass().getName() + " (toString failed: " + e.getClass().getName() + ")";
        }
    }

    private static String truncateUtf8(String text, int maxBytes) {
        byte[] utf8 = text.getBytes(StandardCharsets.UTF_8);
        if (utf8.length <= maxBytes) {
            return text;
        }
        int end = maxBytes;
        // Back off any UTF-8 continuation bytes so the cut keeps the text valid.
        while (end > 0 && (utf8[end] & 0xC0) == 0x80) {
            end--;
        }
        return new String(utf8, 0, end, StandardCharsets.UTF_8) + "... (truncated)";
    }
}
