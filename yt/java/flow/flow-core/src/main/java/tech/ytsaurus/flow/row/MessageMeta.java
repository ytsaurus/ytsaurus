package tech.ytsaurus.flow.row;

import org.jspecify.annotations.Nullable;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeConvertible;
import tech.ytsaurus.ysontree.YTreeNode;

public class MessageMeta implements YTreeConvertible {
    // Null for locally built output messages; assigned at worker side. Always set on input messages.
    private final @Nullable String messageId;
    private final long eventTimestamp;
    private final long systemTimestamp;
    private final String streamId;
    // Identifier of streamId. Shouldn't be filled manually in client's code.
    private final long streamSpecId;

    public MessageMeta(
            String messageId,
            long eventTimestamp,
            long systemTimestamp,
            String streamId,
            long streamSpecId
    ) {
        this.messageId = messageId;
        this.eventTimestamp = eventTimestamp;
        this.systemTimestamp = systemTimestamp;
        this.streamId = streamId;
        this.streamSpecId = streamSpecId;
    }

    MessageMeta(Builder<?> builder) {
        this.messageId = builder.messageId;
        this.eventTimestamp = builder.eventTimestamp;
        this.systemTimestamp = builder.systemTimestamp;
        this.streamId = builder.streamId;
        this.streamSpecId = builder.streamSpecId;
    }

    public static Builder<?> builder() {
        return new Builder<>();
    }

    /**
     * @return Message ID; null for locally built output messages (assigned at worker side).
     */
    public @Nullable String getMessageId() {
        return messageId;
    }

    public long getEventTimestamp() {
        return eventTimestamp;
    }

    public long getSystemTimestamp() {
        return systemTimestamp;
    }

    public String getStreamId() {
        return streamId;
    }

    public long getStreamSpecId() {
        return streamSpecId;
    }

    @Override
    public String toString() {
        return toYTree().toString();
    }

    @Override
    public YTreeNode toYTree() {
        return YTree.builder().beginMap()
                .key("message_id").value(messageId)
                .key("event_timestamp").value(eventTimestamp)
                .key("system_timestamp").value(systemTimestamp)
                .key("stream_id").value(streamId)
                .key("stream_spec_id").value(streamSpecId)
                .endMap()
                .build();
    }

    public static class Builder<B extends MessageMeta.Builder<B>> {
        private @Nullable String messageId;
        private long eventTimestamp;
        private long systemTimestamp;
        private @Nullable String streamId;
        private long streamSpecId;

        @SuppressWarnings("unchecked")
        protected B self() {
            return (B) this;
        }

        public B setMessageId(String messageId) {
            this.messageId = messageId;
            return self();
        }

        public B setEventTimestamp(long eventTimestamp) {
            this.eventTimestamp = eventTimestamp;
            return self();
        }

        public B setSystemTimestamp(long systemTimestamp) {
            this.systemTimestamp = systemTimestamp;
            return self();
        }

        public B setStreamId(String streamId) {
            this.streamId = streamId;
            return self();
        }

        public B setStreamSpecId(long streamSpecId) {
            this.streamSpecId = streamSpecId;
            return self();
        }

        public MessageMeta build() {
            return new MessageMeta(this);
        }
    }
}
