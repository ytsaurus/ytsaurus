package tech.ytsaurus.flow.state;

import org.jspecify.annotations.Nullable;
import tech.ytsaurus.flow.row.Payload;

/**
 * {@link StateAccessor} for an external state.
 */
public class ExternalStateAccessor implements StateAccessor<Payload> {
    final StatesHolder statesHolder;
    private final Payload key;

    /**
     * Intended to be called from {@link StateDescriptor#create}.
     */
    ExternalStateAccessor(
            Payload key,
            StatesHolder statesHolder
    ) {
        statesHolder.requireRowFormat();
        this.statesHolder = statesHolder;
        this.key = key;
    }

    /**
     * {@inheritDoc}
     *
     * @throws UnsupportedOperationException if a value is stored and the holder has no schema.
     */
    @Override
    public @Nullable Payload get() {
        State state = statesHolder.get(key.getRow());
        if (state == null || state.isReset()) {
            return null;
        }
        return state.getValue(statesHolder.valueCodec());
    }

    /**
     * Get state or create an empty Payload with required schema.
     *
     * @return Payload.
     * @throws UnsupportedOperationException if no value is present and the holder has no schema.
     */
    public Payload getOrDefault() {
        Payload value = get();
        return value != null ? value : statesHolder.emptyStatePayload();
    }

    /**
     * {@inheritDoc}
     *
     * @throws UnsupportedOperationException if the holder has no schema.
     */
    @Override
    public void set(Payload value) {
        statesHolder.set(
                key.getRow(),
                new State(statesHolder.encodeValue(value), value)
        );
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
    public Class<Payload> getStateClass() {
        return Payload.class;
    }
}
