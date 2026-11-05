package tech.ytsaurus.flow.state;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import org.jspecify.annotations.Nullable;
import tech.ytsaurus.flow.row.Payload;
import tech.ytsaurus.flow.row.codec.ByteStringCodec;

/**
 * {@link StateAccessor} for an external state in the {@link StateFormat#PROTO} wire format.
 *
 * <p>{@link #get()} returns the parsed state message; {@link #set} writes a message back with
 * dirty tracking (only written states are sent to the worker); {@link #clear()} resets the
 * backing profile. Protobuf messages are immutable, so mutation is expressed as
 * {@code set(get().toBuilder()...build())} or via {@link #getOrDefault()}.
 *
 * @param <T> protobuf message type of the state
 */
public class ProtoStateAccessor<T extends Message> implements StateAccessor<T> {
    private final StatesHolder statesHolder;
    private final Payload key;
    private final Class<T> stateClass;
    private final T defaultInstance;
    private final ByteStringCodec<T> codec;

    /**
     * Intended to be called from {@link StateDescriptor#create}.
     */
    ProtoStateAccessor(
            Payload key,
            StatesHolder statesHolder,
            Class<T> stateClass,
            T defaultInstance,
            ByteStringCodec<T> codec
    ) {
        this.statesHolder = statesHolder;
        this.key = key;
        this.stateClass = stateClass;
        this.defaultInstance = defaultInstance;
        this.codec = codec;
        validateProtoType();
    }

    /**
     * Builds the codec between messages of {@code defaultInstance}'s type and their serialized
     * bytes for the state {@code name}. Descriptors memoize it so every accessor over a
     * descriptor shares one instance.
     */
    @SuppressWarnings("unchecked")
    static <T extends Message> ByteStringCodec<T> codecOf(String name, T defaultInstance) {
        Parser<T> parser = (Parser<T>) defaultInstance.getParserForType();
        String typeName = defaultInstance.getDescriptorForType().getFullName();
        return new ByteStringCodec<>() {
            @Override
            public T decode(ByteString wire) {
                try {
                    return parser.parseFrom(wire);
                } catch (InvalidProtocolBufferException e) {
                    throw new IllegalStateException(
                            "Failed to parse external state %s as %s".formatted(name, typeName), e);
                }
            }

            @Override
            public ByteString encode(T value) {
                return value.toByteString();
            }
        };
    }

    /**
     * Validates the worker-declared proto type against the descriptor's message class. The type
     * is only known for holders mapped from the wire; harness-built holders may carry none.
     *
     * <p>A row-format holder carrying a state schema definitely holds wire rows, not serialized
     * messages — a proto accessor over it is a descriptor/holder mismatch. A schema-less
     * SIMPLE_ROW holder stays legal: absent-state fallback holders are built that way.
     */
    private void validateProtoType() {
        if (statesHolder.getFormat() != StateFormat.PROTO && statesHolder.getStateSchema() != null) {
            throw new IllegalStateException(
                    "External state %s is in the row wire format; use a row state descriptor"
                            .formatted(statesHolder.getName())
            );
        }
        String declaredType = statesHolder.getProtoType();
        String registeredType = defaultInstance.getDescriptorForType().getFullName();
        if (declaredType != null && !declaredType.isEmpty() && !declaredType.equals(registeredType)) {
            throw new IllegalStateException(
                    "Proto type mismatch for external state %s: the worker sends %s while the descriptor uses %s"
                            .formatted(statesHolder.getName(), declaredType, registeredType)
            );
        }
    }

    /**
     * {@inheritDoc}
     *
     * <p>{@code null} means the key has no state entry or the state was reset. A present
     * message with all-default fields is a legitimate state distinct from an absent one.
     */
    @Override
    public @Nullable T get() {
        State state = statesHolder.get(key.getRow());
        if (state == null || state.isReset()) {
            return null;
        }
        return state.getValue(codec);
    }

    /**
     * Get state or the default (all-default fields) message when no value is present.
     *
     * @return the state message
     */
    public T getOrDefault() {
        T value = get();
        return value != null ? value : defaultInstance;
    }

    /**
     * {@inheritDoc}
     *
     * <p>An all-default message serializes to zero bytes and is still stored as a value: the
     * proto wire format keeps such a payload distinct from an absent one.
     */
    @Override
    public void set(T value) {
        // Serialized once, when the response is built, however many times the key is set.
        statesHolder.set(key.getRow(), new State(value, codec));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void clear() {
        statesHolder.clear(key.getRow());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Class<T> getStateClass() {
        return stateClass;
    }
}
