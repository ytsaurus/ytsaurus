package tech.ytsaurus.flow.row.codec;

import com.google.protobuf.ByteString;
import tech.ytsaurus.flow.internal.row.TypeAwareProtoWireProtocolReader;
import tech.ytsaurus.flow.internal.row.TypeAwareProtoWireProtocolWriter;
import tech.ytsaurus.flow.typeinfo.TypeInfo;

/**
 * Default {@link TypedPayloadCodec} that (de)serializes typed-stream payloads using the Y-T
 * proto wire protocol (via {@link TypeAwareProtoWireProtocolReader} /
 * {@link TypeAwareProtoWireProtocolWriter}).
 *
 * <p>The codec is stateless and thread-safe; use {@link #INSTANCE} to avoid allocations.
 */
public final class TypeAwareProtoWireTypedPayloadCodec implements TypedPayloadCodec {

    /**
     * Shared stateless instance.
     */
    public static final TypeAwareProtoWireTypedPayloadCodec INSTANCE = new TypeAwareProtoWireTypedPayloadCodec();

    private TypeAwareProtoWireTypedPayloadCodec() {
    }

    /**
     * Returns a typed codec specialised for the supplied type.
     *
     * @param typeInfo runtime description of the domain type {@code T}
     * @param <T>      domain value type
     * @return a type-bound codec
     */
    @Override
    public <T> ByteStringCodec<T> codecFor(TypeInfo<T> typeInfo) {
        return new ProtoByteStringTypedCodec<>(typeInfo);
    }

    private static final class ProtoByteStringTypedCodec<T> implements ByteStringCodec<T> {
        private final TypeAwareProtoWireProtocolReader<T> reader;
        private final TypeAwareProtoWireProtocolWriter<T> writer;

        ProtoByteStringTypedCodec(TypeInfo<T> typeInfo) {
            this.reader = new TypeAwareProtoWireProtocolReader<>(typeInfo);
            this.writer = new TypeAwareProtoWireProtocolWriter<>(typeInfo);
        }

        @Override
        public T decode(ByteString bytes) {
            return reader.readChunk(bytes);
        }

        @Override
        public ByteString encode(T entity) {
            return writer.writeEntity(entity);
        }
    }
}
