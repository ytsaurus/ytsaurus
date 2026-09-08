package tech.ytsaurus.flow.state;

import java.util.Optional;

import tech.ytsaurus.flow.row.Payload;
import tech.ytsaurus.flow.row.codec.ByteArrayCodec;
import tech.ytsaurus.flow.row.codec.CodecRegistry;
import tech.ytsaurus.flow.row.codec.InternalStateValueCodec;

/**
 * {@link StateAccessor} for the Flow internal state.
 *
 * @param <T> state value type.
 */
public final class InternalStateAccessor<T> implements StateAccessor<T> {
    private final Payload key;
    private final InternalStateDescriptor<T> descriptor;
    private final ByteArrayCodec<T> codec;
    private final InternalStateValueCodec wireCodec;
    private final StatesHolder statesHolder;

    InternalStateAccessor(
            Payload key,
            InternalStateDescriptor<T> descriptor,
            StatesHolder statesHolder
    ) {
        this(key, descriptor, statesHolder, CodecRegistry.getInstance().getInternalStateValueCodec());
    }

    InternalStateAccessor(
            Payload key,
            InternalStateDescriptor<T> descriptor,
            StatesHolder statesHolder,
            InternalStateValueCodec wireCodec
    ) {
        this.key = key;
        this.descriptor = descriptor;
        this.codec = descriptor.getCodec();
        this.wireCodec = wireCodec;
        this.statesHolder = statesHolder;
    }

    /**
     * {@inheritDoc}
     *
     * <p>Decodes the value on every call — its type may be mutable, and a change made to the
     * returned object reaches the state only through {@link #set} — from the entry bytes, which
     * are taken out of the wire once.
     */
    @Override
    public Optional<T> get() {
        State state = statesHolder.get(key.getRow());
        if (state == null || state.isReset()) {
            return Optional.empty();
        }
        return Optional.of(codec.decode(state.getValue(wireCodec)));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void set(T value) {
        byte[] bytes = codec.encode(value);
        statesHolder.set(key.getRow(), new State(wireCodec.encode(bytes), bytes));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void clear() {
        statesHolder.set(key.getRow(), State.RESET);
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
