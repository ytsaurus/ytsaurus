package tech.ytsaurus.flow.context;

import com.google.protobuf.Message;
import tech.ytsaurus.flow.row.Keyed;
import tech.ytsaurus.flow.state.ExternalStateAccessor;
import tech.ytsaurus.flow.state.ExternalStateDescriptor;
import tech.ytsaurus.flow.state.JoinedExternalStateDescriptor;
import tech.ytsaurus.flow.state.JoinedProtoExternalStateDescriptor;
import tech.ytsaurus.flow.state.ProtoExternalStateDescriptor;
import tech.ytsaurus.flow.state.ProtoStateAccessor;
import tech.ytsaurus.flow.state.ReadOnlyExternalStateAccessor;
import tech.ytsaurus.flow.state.ReadOnlyProtoStateAccessor;
import tech.ytsaurus.flow.state.StateAccessor;
import tech.ytsaurus.flow.state.StateDescriptor;

/**
 * Context exposing per-key state access.
 *
 * <p>The single entry point is {@link #getState(StateDescriptor, Keyed)}: the descriptor
 * describes the kind of state (serialization strategy, name, type), the key is taken from a
 * message or timer.
 */
public interface StatefulContext {
    /**
     * Returns the {@link StateAccessor} produced by {@code descriptor}, bound to the key
     * extracted from {@code key}.
     *
     * @param <T> The type of the state value.
     */
    <T> StateAccessor<T> getState(StateDescriptor<T> descriptor, Keyed key);

    /**
     * Returns the {@link ExternalStateAccessor} produced by {@code descriptor}, bound to the key
     * extracted from {@code key}.
     */
    ExternalStateAccessor getState(ExternalStateDescriptor descriptor, Keyed key);

    /**
     * Returns the read-only {@link ReadOnlyExternalStateAccessor} produced by {@code descriptor},
     * bound to the key extracted from {@code key}.
     */
    ReadOnlyExternalStateAccessor getState(JoinedExternalStateDescriptor descriptor, Keyed key);

    /**
     * Returns the {@link ProtoStateAccessor} produced by {@code descriptor}, bound to the key
     * extracted from {@code key}.
     *
     * @param <T> The protobuf message type of the state.
     */
    <T extends Message> ProtoStateAccessor<T> getState(ProtoExternalStateDescriptor<T> descriptor, Keyed key);

    /**
     * Returns the read-only {@link ReadOnlyProtoStateAccessor} produced by {@code descriptor},
     * bound to the key extracted from {@code key}.
     *
     * @param <T> The protobuf message type of the state.
     */
    <T extends Message> ReadOnlyProtoStateAccessor<T> getState(
            JoinedProtoExternalStateDescriptor<T> descriptor,
            Keyed key
    );
}
