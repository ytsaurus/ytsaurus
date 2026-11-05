package tech.ytsaurus.flow.internal.row;

/**
 * Thrown when bytes cannot be decoded as a compact row: a value is truncated, a varint is invalid
 * or a value type is unknown.
 *
 * <p>Extends {@link IllegalArgumentException}. The legacy wire readers throw that only for an
 * unknown value type; they report truncated input and invalid varints as
 * {@link java.io.UncheckedIOException}.
 */
public final class MalformedRowException extends IllegalArgumentException {
    MalformedRowException(String message) {
        super(message);
    }
}
