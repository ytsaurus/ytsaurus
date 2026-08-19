package tech.ytsaurus.flow.state;

import java.util.Optional;

import tech.ytsaurus.flow.row.Payload;

/**
 * {@link StateAccessor} for an external state.
 */
public class ExternalStateAccessor implements StateAccessor<Payload> {
    private final StatesHolder<ExternalState> statesHolder;
    private final Payload key;

    /**
     * Intended to be called from {@link StateDescriptor#create}.
     */
    ExternalStateAccessor(
            Payload key,
            StatesHolder<ExternalState> statesHolder
    ) {
        this.statesHolder = statesHolder;
        this.key = key;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<Payload> get() {
        ExternalState state = statesHolder.get(key.getRow());
        if (state == null || state.isReset() || state.getValue() == null) {
            return Optional.empty();
        }
        return Optional.of(state.getValue());
    }

    /**
     * Get state or create an empty Payload with required schema.
     *
     * @return Payload.
     * @throws UnsupportedOperationException if no value is present and the holder has no schema.
     */
    public Payload getOrDefault() {
        Optional<Payload> value = get();
        return value.isPresent() ? value.get() : statesHolder.emptyStatePayload();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void set(Payload value) {
        statesHolder.set(
                key.getRow(),
                new ExternalState(false, value)
        );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void clear() {
        statesHolder.set(
                key.getRow(),
                ExternalState.RESET
        );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Class<Payload> getStateClass() {
        return Payload.class;
    }
}
