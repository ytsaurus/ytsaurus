package tech.ytsaurus.flow.state;

import tech.ytsaurus.flow.row.Keyed;

/**
 * Describes a state used from user code: its name, value type, and the concrete
 * {@link StateAccessor} returned by
 * {@link tech.ytsaurus.flow.context.StatefulContext#getState}.
 *
 * <p>Descriptors are typically declared as {@code static final} constants and reused when
 * accessing state:
 * <pre>{@code
 * private static final InternalStateDescriptor<TWordCountState> COUNTER =
 *         StateDescriptors.protobuf("word-state", TWordCountState.class);
 *
 * StateAccessor<TWordCountState> acc = ctx.getState(COUNTER, message);
 * }</pre>
 *
 * @param <T> state value type.
 */
public abstract class StateDescriptor<T> {
    /**
     * Returns the state name.
     */
    public abstract String getName();

    /**
     * Returns the state value type.
     */
    public abstract Class<T> getStateClass();

    /**
     * Creates a {@link StateAccessor} bound to {@code key} on top of {@code backend}.
     * Called by the runtime; user code uses
     * {@link tech.ytsaurus.flow.context.StatefulContext#getState}.
     *
     * @param key     The key to bind the state accessor to.
     * @param backend The state backend to use for the state accessor.
     */
    abstract StateAccessor<T> create(Keyed key, StateBackend backend);
}
