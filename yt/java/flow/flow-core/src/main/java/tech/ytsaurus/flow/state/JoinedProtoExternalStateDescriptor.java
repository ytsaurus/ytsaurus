package tech.ytsaurus.flow.state;

import com.google.protobuf.Message;
import tech.ytsaurus.flow.row.Keyed;

/**
 * {@link StateDescriptor} for read-only proto-format external state joined from another
 * computation (must be declared under {@code external_state_joiners} of the computation spec).
 *
 * <p>Joined items are looked up by the message's own key: the companion protocol carries no
 * per-message resolved join key, so a joiner configured with {@code key_schema_override} cannot
 * serve a companion computation and is refused by the worker at job init.
 *
 * @param <T> protobuf message type of the state
 */
public final class JoinedProtoExternalStateDescriptor<T extends Message> extends StateDescriptor<T> {
    private final String name;
    private final Class<T> stateClass;
    private final T defaultInstance;

    JoinedProtoExternalStateDescriptor(String name, Class<T> stateClass) {
        ExternalStateDescriptor.validateExternalStateName(name);
        this.name = name;
        this.stateClass = stateClass;
        this.defaultInstance = ProtoExternalStateDescriptor.defaultInstanceOf(stateClass);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Class<T> getStateClass() {
        return stateClass;
    }

    @Override
    ReadOnlyProtoStateAccessor<T> create(Keyed key, StateBackend backend) {
        return new ReadOnlyProtoStateAccessor<>(
                key.getKey(), backend.getJoinedExternalStateHolder(name), stateClass, defaultInstance);
    }
}
