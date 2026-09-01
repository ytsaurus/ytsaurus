package tech.ytsaurus.flow.state;

import java.util.Optional;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import tech.ytsaurus.flow.row.Payload;

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
    private final StatesHolder<ExternalState> statesHolder;
    private final Payload key;
    private final Class<T> stateClass;
    private final T defaultInstance;

    /**
     * Intended to be called from {@link StateDescriptor#create}.
     */
    ProtoStateAccessor(
            Payload key,
            StatesHolder<ExternalState> statesHolder,
            Class<T> stateClass,
            T defaultInstance
    ) {
        this.statesHolder = statesHolder;
        this.key = key;
        this.stateClass = stateClass;
        this.defaultInstance = defaultInstance;
        validateProtoType();
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
     * <p>An empty optional means the key has no state entry or the state was reset. A present
     * message with all-default fields is a legitimate state distinct from an absent one.
     */
    @Override
    public Optional<T> get() {
        ExternalState state = statesHolder.get(key.getRow());
        if (state == null || state.isReset()) {
            return Optional.empty();
        }
        if (!(state instanceof ProtoExternalState protoState)) {
            throw new IllegalStateException(
                    "External state %s is not in the proto wire format".formatted(statesHolder.getName())
            );
        }
        return Optional.of(parse(protoState));
    }

    /**
     * Get state or the default (all-default fields) message when no value is present.
     *
     * @return the state message
     */
    public T getOrDefault() {
        return get().orElse(defaultInstance);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void set(T value) {
        statesHolder.set(key.getRow(), new ProtoExternalState(value));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void clear() {
        statesHolder.set(key.getRow(), ProtoExternalState.RESET);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Class<T> getStateClass() {
        return stateClass;
    }

    @SuppressWarnings("unchecked")
    private T parse(ProtoExternalState state) {
        Message message = state.getMessage();
        if (message == null) {
            // Received entries cache their parse: repeated reads of the same
            // (immutable) entry must not reparse identical bytes.
            message = state.getParsed();
        }
        if (message != null) {
            if (!stateClass.isInstance(message)) {
                throw new IllegalStateException(
                        "External state %s holds a message of type %s, expected %s"
                                .formatted(
                                        statesHolder.getName(),
                                        message.getClass().getName(),
                                        stateClass.getName())
                );
            }
            return (T) message;
        }
        try {
            T parsed = (T) defaultInstance.getParserForType().parseFrom(state.serialize());
            state.setParsed(parsed);
            return parsed;
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalStateException(
                    "Failed to parse external state %s as %s"
                            .formatted(
                                    statesHolder.getName(),
                                    defaultInstance.getDescriptorForType().getFullName()),
                    e
            );
        }
    }
}
