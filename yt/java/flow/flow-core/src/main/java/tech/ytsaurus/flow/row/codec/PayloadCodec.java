package tech.ytsaurus.flow.row.codec;

import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.flow.row.Payload;

/**
 * Pluggable factory of schema-bound codecs that (de)serialize {@link Payload} blobs between
 * their domain form and their wire {@link com.google.protobuf.ByteString} form.
 *
 * <p>A {@code Payload} is structurally a row described by a {@link TableSchema}; the schema is
 * fixed for the lifetime of the returned codec, allowing implementations to pre-compute
 * per-schema metadata once and reuse it across all encode/decode calls.
 *
 * <p>Implementations and the produced codecs must both be thread-safe.
 *
 * @see CodecRegistry#getPayloadCodec()
 */
public interface PayloadCodec {

    /**
     * Returns a {@link ByteStringCodec} specialised for the given table schema.
     *
     * <p>The returned codec attaches the supplied schema to every decoded {@link Payload};
     * encoding is schema-independent and reads only the row out of the supplied payload.
     *
     * @param schema schema describing the structure of payloads handled by the returned codec;
     *               must not be {@code null}
     * @return a thread-safe codec bound to {@code schema}
     */
    ByteStringCodec<Payload> codecFor(TableSchema schema);
}
