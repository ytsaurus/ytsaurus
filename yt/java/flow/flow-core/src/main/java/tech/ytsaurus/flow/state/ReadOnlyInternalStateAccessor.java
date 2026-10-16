package tech.ytsaurus.flow.state;

import org.jspecify.annotations.Nullable;

/**
 * Read-only {@link InternalStateAccessor}, returned by {@link InternalStateAccessor#readOnly()}.
 *
 * <p>The value it returns is the same object the writable accessor hands out, but reading it here
 * does not make the state tracked: a state the computation only reads is not re-encoded at the
 * end of the request. The default of {@link #getOrDefault} is not attached to the key either, so
 * a later {@link #get} still reports the state absent.
 *
 * @param <T> state value type.
 */
final class ReadOnlyInternalStateAccessor<T> extends InternalStateAccessor<T> {
    ReadOnlyInternalStateAccessor(InternalStateAccessor<T> accessor) {
        super(accessor.key, accessor.descriptor, accessor.statesHolder, accessor.codec);
    }

    @Override
    public @Nullable T get() {
        State state = statesHolder.get(key.getRow());
        if (state == null || state.isReset()) {
            return null;
        }
        return state.getValue(codec);
    }

    @Override
    public T getOrDefault(T defaultValue) {
        T value = get();
        return value != null ? value : defaultValue;
    }

    @Override
    public T getOrDefault() {
        T value = get();
        return value != null ? value : descriptor.defaultValue();
    }

    @Override
    public void set(T value) {
        throw new UnsupportedOperationException(
                "Internal state is read-only (StatesName: %s)".formatted(statesHolder.getName())
        );
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException(
                "Internal state is read-only (StatesName: %s)".formatted(statesHolder.getName())
        );
    }

    @Override
    public StateAccessor<T> readOnly() {
        return this;
    }
}
