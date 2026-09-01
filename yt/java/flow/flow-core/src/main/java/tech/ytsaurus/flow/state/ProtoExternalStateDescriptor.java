package tech.ytsaurus.flow.state;

import com.google.protobuf.Message;
import tech.ytsaurus.flow.row.Keyed;

/**
 * {@link StateDescriptor} for an external state in the {@link StateFormat#PROTO} wire format
 * (must be pre-configured in the computation spec and served by a proto-capable state manager
 * on the worker side).
 *
 * @param <T> protobuf message type of the state
 */
public final class ProtoExternalStateDescriptor<T extends Message> extends StateDescriptor<T> {
    private final String name;
    private final Class<T> stateClass;
    private final T defaultInstance;

    ProtoExternalStateDescriptor(String name, Class<T> stateClass) {
        ExternalStateDescriptor.validateExternalStateName(name);
        this.name = name;
        this.stateClass = stateClass;
        this.defaultInstance = defaultInstanceOf(stateClass);
    }

    static <T extends Message> T defaultInstanceOf(Class<T> messageClass) {
        try {
            return messageClass.cast(messageClass.getMethod("getDefaultInstance").invoke(null));
        } catch (ReflectiveOperationException e) {
            throw new IllegalArgumentException(
                    "Cannot resolve the default instance of " + messageClass.getName(), e);
        }
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
    ProtoStateAccessor<T> create(Keyed key, StateBackend backend) {
        return new ProtoStateAccessor<>(
                key.getKey(), backend.getExternalStateHolder(name), stateClass, defaultInstance);
    }
}
