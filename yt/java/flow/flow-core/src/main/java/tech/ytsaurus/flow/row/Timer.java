package tech.ytsaurus.flow.row;

import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeNode;

/**
 * Represents NYT::NFlow::TTimer.
 */
public class Timer extends MessageMeta implements Keyed {
    private final long triggerTimestamp;
    private final Payload key;

    public Timer(
            String messageId,
            long eventTimestamp,
            long systemTimestamp,
            String streamId,
            long streamSpecId,
            long triggerTimestamp,
            Payload key
    ) {
        super(messageId, eventTimestamp, systemTimestamp, streamId, streamSpecId);
        this.triggerTimestamp = triggerTimestamp;
        this.key = key;
    }

    Timer(Builder<?> builder) {
        super(builder);
        this.triggerTimestamp = builder.triggerTimestamp;
        this.key = builder.key;
    }

    public static Builder<?> builder() {
        return new Builder<>();
    }

    public long getTriggerTimestamp() {
        return triggerTimestamp;
    }

    @Override
    public Payload getKey() {
        return key;
    }


    @Override
    public YTreeNode toYTree() {
        var ytree = super.toYTree();
        ytree.mapNode().put("trigger_timestamp", YTree.longNode(triggerTimestamp));
        ytree.mapNode().put("key", key.toYTree());
        return ytree;
    }

    public static class Builder<T extends Timer.Builder<T>> extends MessageMeta.Builder<T> {
        private long triggerTimestamp;
        private Payload key;

        @SuppressWarnings("unchecked")
        @Override
        protected T self() {
            return (T) this;
        }

        public T setTriggerTimestamp(long triggerTimestamp) {
            this.triggerTimestamp = triggerTimestamp;
            return self();
        }

        public T setKey(Payload key) {
            this.key = key;
            return self();
        }

        @Override
        public Timer build() {
            return new Timer(this);
        }
    }
}
