package tech.ytsaurus.flow.stream;

import java.util.Objects;

import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.flow.row.codec.ByteStringCodec;

/**
 * Default {@link FlowStream} implementation that delegates (de)serialization to a
 * {@link ByteStringCodec} bound at construction time.
 *
 * @param <T> the type of messages in this stream
 */
public final class DefaultFlowStream<T> implements FlowStream<T> {

    private final String streamId;
    private final TableSchema schema;
    private final Class<T> messageClass;
    private final ByteStringCodec<T> codec;

    /**
     * Package-private constructor; instances are obtained via {@link FlowStreams}.
     *
     * @param streamId     the unique stream identifier
     * @param schema       the table schema for messages on this stream
     * @param messageClass the runtime class of messages
     * @param codec        the bound (de)serialization codec
     */
    DefaultFlowStream(
            String streamId,
            TableSchema schema,
            Class<T> messageClass,
            ByteStringCodec<T> codec
    ) {
        this.streamId = Objects.requireNonNull(streamId, "streamId must not be null");
        this.schema = Objects.requireNonNull(schema, "schema must not be null");
        this.messageClass = Objects.requireNonNull(messageClass, "messageClass must not be null");
        this.codec = Objects.requireNonNull(codec, "codec must not be null");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getStreamId() {
        return streamId;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TableSchema getSchema() {
        return schema;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Class<T> getMessageClass() {
        return messageClass;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ByteStringCodec<T> getCodec() {
        return codec;
    }
}
