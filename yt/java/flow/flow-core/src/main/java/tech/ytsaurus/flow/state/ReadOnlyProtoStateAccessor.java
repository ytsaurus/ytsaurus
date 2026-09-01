package tech.ytsaurus.flow.state;

import com.google.protobuf.Message;
import tech.ytsaurus.flow.row.Payload;

/**
 * Read-only {@link ProtoStateAccessor} for proto-format external state joined from another
 * computation: reads work as usual, writes throw — joiners never write back.
 *
 * @param <T> protobuf message type of the state
 */
public final class ReadOnlyProtoStateAccessor<T extends Message> extends ProtoStateAccessor<T> {

    /**
     * Intended to be called from {@link StateDescriptor#create}.
     */
    ReadOnlyProtoStateAccessor(
            Payload key,
            StatesHolder<ExternalState> statesHolder,
            Class<T> stateClass,
            T defaultInstance
    ) {
        super(key, statesHolder, stateClass, defaultInstance);
    }

    @Override
    public void set(T value) {
        throw new UnsupportedOperationException("Joined external state is read-only; joiners never write back");
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException("Joined external state is read-only; joiners never write back");
    }
}
