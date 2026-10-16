package tech.ytsaurus.flow.computation;

import java.util.Objects;

/** Selects how a Swift computation identifies sibling output messages. */
public final class MessageIdSuffix {
    public enum Mode {
        SEQUENCE_NUMBER,
        PAYLOAD_HASH,
        USER_DEFINED,
    }

    private static final MessageIdSuffix SEQUENCE_NUMBER = new MessageIdSuffix(Mode.SEQUENCE_NUMBER, "");
    private static final MessageIdSuffix PAYLOAD_HASH = new MessageIdSuffix(Mode.PAYLOAD_HASH, "");

    private final Mode mode;
    private final String value;

    private MessageIdSuffix(Mode mode, String value) {
        this.mode = mode;
        this.value = value;
    }

    public static MessageIdSuffix sequenceNumber() {
        return SEQUENCE_NUMBER;
    }

    public static MessageIdSuffix payloadHash() {
        return PAYLOAD_HASH;
    }

    public static MessageIdSuffix userDefined(String value) {
        Objects.requireNonNull(value, "value");
        if (value.isEmpty()) {
            throw new IllegalArgumentException("User-defined output message ID suffix must not be empty");
        }
        return new MessageIdSuffix(Mode.USER_DEFINED, value);
    }

    public Mode getMode() {
        return mode;
    }

    public String getValue() {
        return value;
    }
}
