package tech.ytsaurus.flow.state;

import java.util.Optional;

import tech.ytsaurus.flow.row.Payload;

/**
 * Read-only {@link StateAccessor} over external state joined from another computation.
 *
 * <p>{@link #get()} and {@link #getOrDefault()} read the joined value; {@link #set} and
 * {@link #clear} throw {@link UnsupportedOperationException} because a joiner is not a write-owner
 * of the underlying state table.
 */
public class ReadOnlyExternalStateAccessor implements StateAccessor<Payload> {
    private final StatesHolder<ExternalState> statesHolder;
    private final Payload key;

    /**
     * Intended to be called from {@link StateDescriptor#create}.
     */
    ReadOnlyExternalStateAccessor(
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
     * Get joined state or an empty Payload with the joined state's schema.
     *
     * @return Payload.
     * @throws UnsupportedOperationException if no value was joined and the holder has no schema.
     */
    public Payload getOrDefault() {
        Optional<Payload> value = get();
        return value.isPresent() ? value.get() : statesHolder.emptyStatePayload();
    }

    /**
     * Always throws: joined external state is read-only.
     */
    @Override
    public void set(Payload value) {
        throw new UnsupportedOperationException(
                "Joined external state is read-only (StatesName: %s)".formatted(statesHolder.getName())
        );
    }

    /**
     * Always throws: joined external state is read-only.
     */
    @Override
    public void clear() {
        throw new UnsupportedOperationException(
                "Joined external state is read-only (StatesName: %s)".formatted(statesHolder.getName())
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
