package tech.ytsaurus.flow.row;

import org.jspecify.annotations.Nullable;
import tech.ytsaurus.core.tables.TableSchema;

/**
 * Builder for {@link Message}.
 * <p>
 * It is supposed to be reused for building all messages in batch in order to minimising data allocations.
 */
public class MessageBuilder {
    private final String streamId;
    private final TableSchema schema;
    private final PayloadBuilder payloadBuilder;
    private long systemTimestamp;
    private long eventTimestamp;

    public MessageBuilder(String streamId, TableSchema schema) {
        this.streamId = streamId;
        this.schema = schema;
        this.payloadBuilder = new PayloadBuilder(schema);
    }

    /**
     * Sets value to an underling UnversionedRow.
     *
     * @param columnName Name of the column from schema.
     * @param value      Value, may be {@code null} (stored as a NULL-typed cell).
     * @return this.
     */
    public <T> MessageBuilder set(String columnName, @Nullable T value) {
        payloadBuilder.set(columnName, value);
        return this;
    }

    /**
     * Sets message system timestamp.
     * <p>
     * In normal circumstances the system timestamp should be left blank and filled at worker side.
     *
     * @param systemTimestamp Timestamp of message creation.
     * @return this.
     */
    public MessageBuilder setSystemTimestamp(long systemTimestamp) {
        this.systemTimestamp = systemTimestamp;
        return this;
    }

    /**
     * Sets message event timestamp.
     * <p>
     * Event timestamp might be left blank and filled at worker side.
     *
     * @param eventTimestamp Timestamp of event.
     * @return this.
     */
    public MessageBuilder setEventTimestamp(long eventTimestamp) {
        this.eventTimestamp = eventTimestamp;
        return this;
    }

    /**
     * Build {@link Message} and reset all underling buffers and variables.
     * <p>
     * {@link MessageBuilder} Can be used for next message building after this method call.
     *
     * @return Message.
     */
    public Message finish() {
        Payload payload = payloadBuilder.finish();
        var message = Message.builder()
                .setStreamId(streamId)
                .setEventTimestamp(eventTimestamp)
                .setSystemTimestamp(systemTimestamp)
                .setPayload(payload)
                .setStreamSpecId(0)
                .build();
        resetTimestamps();
        return message;
    }

    private void resetTimestamps() {
        systemTimestamp = 0;
        eventTimestamp = 0;
    }

    /**
     * @return TableSchema of underling UnversionedRow.
     */
    public TableSchema getSchema() {
        return schema;
    }

    /**
     * @return StreamId of message.
     */
    public String getStreamId() {
        return streamId;
    }
}
