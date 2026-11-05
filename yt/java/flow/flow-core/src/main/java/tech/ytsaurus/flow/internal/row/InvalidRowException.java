package tech.ytsaurus.flow.internal.row;

/**
 * Thrown when a decoded compact row breaks the wire format, its schema or a size limit: an
 * unsupported version, a value count or column id that differs from the schema, a cell type that
 * differs from the schema, a value above the length limit, or trailing bytes.
 *
 * <p>Extends {@link IllegalStateException}, which the legacy wire readers throw for such rows, so
 * existing handlers keep working.
 */
public final class InvalidRowException extends IllegalStateException {
    InvalidRowException(String message) {
        super(message);
    }
}
