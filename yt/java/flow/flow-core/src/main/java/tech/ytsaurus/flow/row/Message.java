package tech.ytsaurus.flow.row;

import org.jspecify.annotations.Nullable;
import tech.ytsaurus.core.utils.ClassUtils;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeConvertible;
import tech.ytsaurus.ysontree.YTreeNode;

/**
 * Java equivalent of NYT::NFlow::TMessage.
 */
public class Message extends MessageMeta {
    private final Object payload;

    /**
     * @deprecated Use Message.builder().setStreamId(streamId).setPayload(payload).build() instead.
     */
    @Deprecated
    public Message(
            String streamId,
            Object payload
    ) {
        this(builder().setStreamId(streamId).setPayload(payload));
    }

    Message(Builder<?> builder) {
        super(builder);
        this.payload = builder.payload;
    }

    public static Builder<?> builder() {
        return new Builder<>();
    }

    // Compatibility mode.
    public <T> @Nullable T get(String columnName, Class<T> fieldClass) {
        return ((Payload) payload).get(columnName, fieldClass);
    }

    // Compatibility mode.
    public <T> @Nullable T get(int columnId, Class<T> fieldClass) {
        return ((Payload) payload).get(columnId, fieldClass);
    }

    public <T> T getPayload() {
        return ClassUtils.castToType(payload);
    }

    @Override
    public YTreeNode toYTree() {
        var ytree = super.toYTree();
        ytree.mapNode().put("payload_class", YTree.stringNode(payload.getClass().getName()));
        if (YTreeConvertible.class.isAssignableFrom(payload.getClass())) {
            ytree.mapNode().put("payload", ((YTreeConvertible) payload).toYTree());
        }
        return ytree;
    }

    public static class Builder<T extends Message.Builder<T>> extends MessageMeta.Builder<T> {
        private @Nullable Object payload;

        @SuppressWarnings("unchecked")
        @Override
        protected T self() {
            return (T) this;
        }

        public T setPayload(Object payload) {
            this.payload = payload;
            return self();
        }

        @Override
        public Message build() {
            return new Message(this);
        }
    }
}
