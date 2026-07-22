package tech.ytsaurus.flow.row.codec;

import tech.ytsaurus.client.rows.UnversionedRow;

/**
 * Pluggable codec that (de)serializes message, timer and state keys ({@link UnversionedRow})
 * between their domain form and their wire {@link com.google.protobuf.ByteString} form.
 *
 * <p>Implementations must be thread-safe: a single instance is shared by all mappers across
 * all concurrent requests handled by the companion server.
 *
 * @see CodecRegistry#getKeyCodec()
 */
public interface KeyCodec extends ByteStringCodec<UnversionedRow> {
}
