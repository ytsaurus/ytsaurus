package tech.ytsaurus.flow.row.codec;

import com.google.protobuf.ByteString;
import tech.ytsaurus.client.rows.UnversionedRow;
import tech.ytsaurus.flow.internal.row.ProtoWireProtocolReader;
import tech.ytsaurus.flow.internal.row.ProtoWireProtocolWriter;

/**
 * Default {@link KeyCodec} that (de)serializes an {@link UnversionedRow} using the Y-T proto
 * wire protocol (via {@link ProtoWireProtocolReader} / {@link ProtoWireProtocolWriter}).
 *
 * <p>The codec is stateless and thread-safe; use {@link #INSTANCE} to avoid allocations.
 */
public final class ProtoByteStringKeyCodec implements KeyCodec {

    /**
     * Shared stateless instance.
     */
    public static final ProtoByteStringKeyCodec INSTANCE = new ProtoByteStringKeyCodec();

    private ProtoByteStringKeyCodec() {
    }

    /**
     * Decodes a wire byte string into an {@link UnversionedRow}.
     *
     * @param bytes the wire byte string
     * @return the decoded row
     */
    @Override
    public UnversionedRow decode(ByteString bytes) {
        return new ProtoWireProtocolReader(bytes).readUnversionedRow();
    }

    /**
     * Encodes an {@link UnversionedRow} into its wire byte string representation.
     *
     * @param row the row to encode
     * @return the encoded byte string
     */
    @Override
    public ByteString encode(UnversionedRow row) {
        return new ProtoWireProtocolWriter(row).getBinary();
    }
}
