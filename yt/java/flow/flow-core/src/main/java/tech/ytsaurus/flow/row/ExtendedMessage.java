package tech.ytsaurus.flow.row;

import org.jspecify.annotations.Nullable;
import tech.ytsaurus.ysontree.YTreeNode;

/**
 * An extended version of {@link Message} with key and key schema.
 *
 * @see Message
 */
public class ExtendedMessage extends Message implements Keyed {
    // Key (with group_by_schema format)
    private final @Nullable Payload key;

    ExtendedMessage(Builder<?> builder) {
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
        var messageNode = super.toYTree();
        if (key != null) {
            messageNode.mapNode().put("key", key.toYTree());
        }
        return messageNode;
    }

    public static class Builder<E extends ExtendedMessage.Builder<E>> extends Message.Builder<E> {
        private @Nullable Payload key;

        @SuppressWarnings("unchecked")
        @Override
        protected E self() {
            return (E) this;
        }

        public E setKey(Payload key) {
            this.key = key;
            return self();
        }

        @Override
        public ExtendedMessage build() {
            return new ExtendedMessage(this);
        }
    }
}
