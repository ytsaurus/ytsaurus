package tech.ytsaurus.flow.row;

import tech.ytsaurus.ysontree.YTreeNode;

/**
 * Represents NYT::NFlow::TVisit — a visit event emitted by a key-visitor stream.
 */
public class Visit extends MessageMeta implements Keyed {
    private final Payload key;

    public Visit(
            String messageId,
            long eventTimestamp,
            long systemTimestamp,
            String streamId,
            Payload key
    ) {
        super(messageId, eventTimestamp, systemTimestamp, streamId, /*streamSpecId*/ -1L);
        this.key = key;
    }

    Visit(Builder<?> builder) {
        super(builder);
        this.key = builder.key;
    }

    public static Builder<?> builder() {
        return new Builder<>();
    }

    @Override
    public Payload getKey() {
        return key;
    }

    @Override
    public YTreeNode toYTree() {
        var ytree = super.toYTree();
        ytree.mapNode().put("key", key.toYTree());
        return ytree;
    }

    public static class Builder<T extends Visit.Builder<T>> extends MessageMeta.Builder<T> {
        private Payload key;

        @SuppressWarnings("unchecked")
        @Override
        protected T self() {
            return (T) this;
        }

        public T setKey(Payload key) {
            this.key = key;
            return self();
        }

        @Override
        public Visit build() {
            return new Visit(this);
        }
    }
}
