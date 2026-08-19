package tech.ytsaurus.flow.state;

import java.util.Optional;

import tech.ytsaurus.flow.row.Payload;
import tech.ytsaurus.flow.row.codec.ByteArrayCodec;

/**
 * {@link StateAccessor} for the Flow internal state.
 *
 * @param <T> state value type.
 */
public final class InternalStateAccessor<T> implements StateAccessor<T> {
    private final Payload key;
    private final InternalStateDescriptor<T> descriptor;
    private final ByteArrayCodec<T> codec;
    private final StatesHolder<InternalState> statesHolder;

    InternalStateAccessor(
            Payload key,
            InternalStateDescriptor<T> descriptor,
            StatesHolder<InternalState> statesHolder
    ) {
        this.key = key;
        this.descriptor = descriptor;
        this.codec = descriptor.getCodec();
        this.statesHolder = statesHolder;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<T> get() {
        InternalState internalState = statesHolder.get(key.getRow());
        if (internalState == null || internalState.isReset() || internalState.getValue() == null) {
            return Optional.empty();
        }
        return Optional.of(codec.decode(internalState.getValue()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void set(T value) {
        statesHolder.set(
                key.getRow(),
                new InternalState(false, codec.encode(value))
        );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void clear() {
        statesHolder.set(key.getRow(), InternalState.RESET);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Class<T> getStateClass() {
        return descriptor.getStateClass();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public T getOrDefault() {
        Optional<T> value = get();
        return value.isPresent() ? value.get() : descriptor.defaultValue();
    }
}
