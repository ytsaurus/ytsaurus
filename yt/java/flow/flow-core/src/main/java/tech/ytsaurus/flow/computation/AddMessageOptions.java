package tech.ytsaurus.flow.computation;

import java.util.Objects;

/** Options for adding an output message. */
public final class AddMessageOptions {
    private static final AddMessageOptions DEFAULTS = new Builder().build();

    private final boolean distribute;
    private final MessageIdSuffix messageIdSuffix;

    private AddMessageOptions(Builder builder) {
        distribute = builder.distribute;
        messageIdSuffix = builder.messageIdSuffix;
    }

    public static AddMessageOptions defaults() {
        return DEFAULTS;
    }

    public static Builder builder() {
        return new Builder();
    }

    public boolean isDistribute() {
        return distribute;
    }

    public MessageIdSuffix getMessageIdSuffix() {
        return messageIdSuffix;
    }

    public static final class Builder {
        private boolean distribute = true;
        private MessageIdSuffix messageIdSuffix = MessageIdSuffix.sequenceNumber();

        public Builder setDistribute(boolean distribute) {
            this.distribute = distribute;
            return this;
        }

        public Builder setMessageIdSuffix(MessageIdSuffix messageIdSuffix) {
            this.messageIdSuffix = Objects.requireNonNull(messageIdSuffix, "messageIdSuffix");
            return this;
        }

        public AddMessageOptions build() {
            return new AddMessageOptions(this);
        }
    }
}
