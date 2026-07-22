package tech.ytsaurus.flow.stream;

import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.flow.row.Payload;
import tech.ytsaurus.flow.row.codec.CodecRegistry;
import tech.ytsaurus.flow.typeinfo.TypeInfo;

/**
 * Factory entry points for {@link FlowStream} instances.
 *
 * <p>{@link #raw(String, TableSchema)} produces untyped {@link Payload} streams;
 * {@link #typed(String, Class)} produces type-aware streams.
 */
public final class FlowStreams {

    private FlowStreams() {
    }

    /**
     * Creates a raw payload stream whose codec is selected from the JVM-wide
     * {@link CodecRegistry#getInstance()}.
     *
     * @param streamId the unique stream identifier
     * @param schema   the table schema for payloads on this stream
     * @return a new raw payload stream
     */
    public static FlowStream<Payload> raw(String streamId, TableSchema schema) {
        var codec = CodecRegistry.getInstance().getPayloadCodec().codecFor(schema);
        return new DefaultFlowStream<>(streamId, schema, Payload.class, codec);
    }

    /**
     * Creates a typed stream whose codec is selected from the JVM-wide
     * {@link CodecRegistry#getInstance()}.
     *
     * @param streamId     the unique stream identifier
     * @param messageClass the runtime class of messages on this stream
     * @param <T>          the type of messages on this stream
     * @return a new typed stream
     */
    public static <T> FlowStream<T> typed(String streamId, Class<T> messageClass) {
        var typeInfo = new TypeInfo<>(messageClass);
        var codec = CodecRegistry.getInstance().getTypedPayloadCodec().codecFor(typeInfo);
        return new DefaultFlowStream<>(streamId, typeInfo.getTableSchema(), messageClass, codec);
    }
}
