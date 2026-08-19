package tech.ytsaurus.flow.row.codec;

import com.google.protobuf.ByteString;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.flow.internal.row.ProtoWireProtocolReader;
import tech.ytsaurus.flow.internal.row.ProtoWireProtocolWriter;
import tech.ytsaurus.flow.row.Payload;

/**
 * Default {@link PayloadCodec} that (de)serializes {@link Payload} blobs using the Y-T proto
 * wire protocol (via {@link ProtoWireProtocolReader} / {@link ProtoWireProtocolWriter}).
 *
 * <p>The wire format is schema-agnostic: the schema is only attached to the decoded
 * {@link Payload} and is never consulted during the actual encode/decode of the underlying
 * {@link tech.ytsaurus.client.rows.UnversionedRow}.
 *
 * <p>The codec is stateless and thread-safe; use {@link #INSTANCE} to avoid allocations.
 */
public final class ProtoWirePayloadCodec implements PayloadCodec {

    /**
     * Shared stateless instance.
     */
    public static final ProtoWirePayloadCodec INSTANCE = new ProtoWirePayloadCodec();

    private ProtoWirePayloadCodec() {
    }

    /**
     * Returns a payload codec that attaches the supplied schema to every decoded payload.
     *
     * @param schema schema to attach to decoded payloads
     * @return a schema-bound payload codec
     */
    @Override
    public ByteStringCodec<Payload> codecFor(TableSchema schema) {
        return new SchemaBoundCodec(schema);
    }

    private static final class SchemaBoundCodec implements ByteStringCodec<Payload> {
        private final TableSchema schema;

        SchemaBoundCodec(TableSchema schema) {
            this.schema = schema;
        }

        @Override
        public Payload decode(ByteString bytes) {
            var row = new ProtoWireProtocolReader(bytes).readUnversionedRow();
            return new Payload(row, schema);
        }

        @Override
        public ByteString encode(Payload payload) {
            return new ProtoWireProtocolWriter(payload.getRow()).getBinary();
        }
    }
}
